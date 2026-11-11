from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task


def _run_data_task(
    action: str,
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    helper = Path("/opt/airflow/support/data_tasks.py")
    python = Path("/opt/data-venv/bin/python")
    if not helper.is_file():
        raise FileNotFoundError(f"Script d'intégration introuvable : {helper}")
    if not python.is_file():
        raise FileNotFoundError(f"Python des traitements introuvable : {python}")

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as output_file:
        result_path = Path(output_file.name)
    try:
        subprocess.run(
            [str(python), str(helper), action, str(result_path)],
            input=json.dumps(payload or {}),
            text=True,
            check=True,
        )
        result = json.loads(result_path.read_text(encoding="utf-8"))
        if not isinstance(result, dict):
            raise TypeError(f"Résultat inattendu du traitement {action}.")
        return result
    finally:
        result_path.unlink(missing_ok=True)


@dag(
    dag_id="dag_excel_ingestion",
    description="Copie les fichiers BI vers D: puis les charge dans PostgreSQL.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["excel", "bi", "ingestion"],
)
def dag_excel_ingestion():
    @task(execution_timeout=timedelta(minutes=20))
    def synchronize_files() -> dict[str, int]:
        script_path = Path(os.environ["BI_COPY_SCRIPT_PATH"])
        if not script_path.is_file():
            raise FileNotFoundError(
                f"Script de synchronisation introuvable : {script_path}"
            )

        spec = importlib.util.spec_from_file_location(
            "bi_file_synchronizer",
            script_path,
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Chargement impossible du script : {script_path}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        result = module.synchroniser()
        if result["deferred"]:
            raise RuntimeError(
                f"{result['deferred']} fichier(s) sont encore en cours d'écriture."
            )
        return result

    @task(execution_timeout=timedelta(minutes=30))
    def ingest_excel_files() -> dict[str, object]:
        return _run_data_task("ingest_excel")

    @task(execution_timeout=timedelta(minutes=5))
    def validate_excel_load(summary: dict[str, object]) -> dict[str, object]:
        return _run_data_task("validate_excel", summary)

    copied = synchronize_files()
    ingested = ingest_excel_files()
    copied >> ingested
    validate_excel_load(ingested)


dag_excel_ingestion()
