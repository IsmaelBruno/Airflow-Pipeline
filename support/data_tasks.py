from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def _excel_modules():
    excel_project_src = Path(os.environ["EXCEL_PROJECT_SRC"])
    if not excel_project_src.is_dir():
        raise FileNotFoundError(
            f"Sources du pipeline Excel introuvables : {excel_project_src}"
        )
    sys.path.insert(0, str(excel_project_src))

    from atg_excel_pipeline.config import load_settings
    from atg_excel_pipeline.database import create_database_engine

    return load_settings, create_database_engine


def ingest_excel() -> dict[str, object]:
    load_settings, create_database_engine = _excel_modules()
    from atg_excel_pipeline.logging_config import configure_logging
    from atg_excel_pipeline.pipeline import scan_once
    from atg_excel_pipeline.repository import ensure_schema

    settings = load_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    try:
        ensure_schema(engine)
        results = scan_once(settings, engine)
        failed = [item for item in results if item.status == "failed"]
        if failed:
            details = ", ".join(item.path.name for item in failed[:5])
            raise RuntimeError(f"{len(failed)} fichier(s) Excel en échec : {details}")
        return {
            "files_scanned": len(results),
            "loaded": sum(item.status == "success" for item in results),
            "skipped": sum(item.status == "skipped" for item in results),
            "successful_files": [
                {"name": item.path.name, "flow": item.flow.value}
                for item in results
                if item.status in {"success", "skipped"}
            ],
        }
    finally:
        engine.dispose()


def validate_excel(summary: dict[str, object]) -> dict[str, object]:
    load_settings, create_database_engine = _excel_modules()
    from sqlalchemy import text

    engine = create_database_engine(load_settings())
    try:
        with engine.connect() as connection:
            schema = connection.execute(text("SELECT current_schema()")).scalar_one()
            table_exists = connection.execute(
                text("SELECT to_regclass('excel_files') IS NOT NULL")
            ).scalar_one()
            if not table_exists:
                raise RuntimeError(
                    f"La table excel_files est absente du schéma {schema}."
                )

            registered_successes = 0
            for item in summary["successful_files"]:
                registered = connection.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM excel_files
                            WHERE source_name = :source_name
                              AND flow = :flow
                              AND status = 'success'
                        )
                        """
                    ),
                    {"source_name": item["name"], "flow": item["flow"]},
                ).scalar_one()
                if not registered:
                    raise RuntimeError(
                        "Le chargement n'est pas audité en succès pour "
                        f"{item['name']} ({item['flow']})."
                    )
                registered_successes += 1
        return {
            "schema": schema,
            "files_scanned": summary["files_scanned"],
            "loaded": summary["loaded"],
            "skipped": summary["skipped"],
            "registered_successes": registered_successes,
        }
    finally:
        engine.dispose()


def detect_atg() -> dict[str, str | None]:
    _, create_database_engine = _excel_modules()
    from atg_excel_pipeline.config import load_settings
    from sqlalchemy import text

    engine = create_database_engine(load_settings())
    try:
        with engine.connect() as connection:
            latest = connection.execute(
                text(
                    """
                    SELECT max(processed_at)
                    FROM public.file_audit
                    WHERE status = 'processed'
                    """
                )
            ).scalar_one()
    finally:
        engine.dispose()
    return {"watermark": None if latest is None else latest.isoformat()}


def dbt_build(payload: dict[str, str]) -> dict[str, str]:
    project_dir = Path(os.environ["DBT_PROJECT_DIR"])
    profiles_dir = Path(os.environ["DBT_PROFILES_DIR"])
    env_file = project_dir / ".env"
    if not project_dir.is_dir():
        raise FileNotFoundError(f"Projet dbt introuvable : {project_dir}")
    if not profiles_dir.is_dir():
        raise FileNotFoundError(f"Profils dbt introuvables : {profiles_dir}")
    if not env_file.is_file():
        raise FileNotFoundError(f"Fichier d'environnement dbt absent : {env_file}")

    from dotenv import dotenv_values

    env = os.environ.copy()
    env.update(
        {
            key: value
            for key, value in dotenv_values(env_file).items()
            if value is not None
        }
    )
    required = (
        "DBT_HOST",
        "DBT_PORT",
        "DBT_DATABASE",
        "DBT_DEV_USER",
        "DBT_DEV_PASSWORD",
    )
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise RuntimeError(
            "Configuration dbt incomplète : " + ", ".join(missing)
        )
    env["DBT_PROFILES_DIR"] = str(profiles_dir)

    runtime_dir = Path("/tmp/airflow-dbt")
    runtime_dir.mkdir(parents=True, exist_ok=True)
    dbt = Path(sys.executable).parent / "dbt"
    subprocess.run(
        [
            str(dbt),
            "build",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
            "--target",
            "dev",
            "--target-path",
            str(runtime_dir / "target"),
            "--log-path",
            str(runtime_dir / "logs"),
        ],
        cwd=project_dir,
        env=env,
        check=True,
    )
    return {"watermark": payload["watermark"]}


def validate_dbt(payload: dict[str, str]) -> dict[str, int | str]:
    _, create_database_engine = _excel_modules()
    from atg_excel_pipeline.config import load_settings
    from sqlalchemy import text

    engine = create_database_engine(load_settings())
    try:
        with engine.connect() as connection:
            source_rows = connection.execute(
                text("SELECT count(*) FROM public.tank_measurements")
            ).scalar_one()
            target_exists = connection.execute(
                text(
                    "SELECT to_regclass('dbt_dev_prod.tank_measurements') IS NOT NULL"
                )
            ).scalar_one()
            if source_rows == 0:
                raise RuntimeError(
                    "La source public.tank_measurements est vide après dbt build."
                )
            if not target_exists:
                raise RuntimeError(
                    "La table dbt_dev_prod.tank_measurements est indisponible."
                )
            target_rows = connection.execute(
                text("SELECT count(*) FROM dbt_dev_prod.tank_measurements")
            ).scalar_one()
        return {
            "source_rows": source_rows,
            "target_rows": target_rows,
            "source_watermark": payload["watermark"],
        }
    finally:
        engine.dispose()


def main() -> None:
    action = sys.argv[1]
    result_path = Path(sys.argv[2])
    payload = json.load(sys.stdin)
    actions = {
        "ingest_excel": lambda: ingest_excel(),
        "validate_excel": lambda: validate_excel(payload),
        "detect_atg": detect_atg,
        "dbt_build": lambda: dbt_build(payload),
        "validate_dbt": lambda: validate_dbt(payload),
    }
    if action not in actions:
        raise ValueError(f"Tâche inconnue : {action}")
    result_path.write_text(
        json.dumps(actions[action](), ensure_ascii=False),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
