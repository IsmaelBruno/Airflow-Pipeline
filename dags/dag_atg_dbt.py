from __future__ import annotations

import json
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.sdk import Variable, dag, task


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
    dag_id="dag_atg_dbt",
    description="Suit les chargements ATG validés dans PostgreSQL puis exécute dbt.",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["atg", "nifi", "dbt"],
)
def dag_atg_dbt():
    @task(execution_timeout=timedelta(minutes=2))
    def detect_completed_atg_load() -> str:
        result = _run_data_task("detect_atg")
        watermark = result.get("watermark")
        if not isinstance(watermark, str):
            raise AirflowSkipException(
                "Aucun chargement ATG réussi n'est encore audité dans PostgreSQL."
            )

        latest = datetime.fromisoformat(watermark)
        previous = Variable.get("atg_dbt_last_processed_at", default=None)
        if previous is not None and latest <= datetime.fromisoformat(previous):
            raise AirflowSkipException(
                "Aucun nouveau chargement ATG réussi depuis le dernier dbt build."
            )

        if latest > datetime.now(timezone.utc) - timedelta(minutes=2):
            raise AirflowSkipException(
                "Un chargement ATG vient de se terminer; le prochain scan vérifiera "
                "que la source est stabilisée."
            )
        return watermark

    @task(execution_timeout=timedelta(hours=1))
    def run_dbt_build(source_watermark: str) -> str:
        _run_data_task("dbt_build", {"watermark": source_watermark})
        return source_watermark

    @task(execution_timeout=timedelta(minutes=5))
    def validate_dbt_output(source_watermark: str) -> dict[str, int | str]:
        result = _run_data_task("validate_dbt", {"watermark": source_watermark})
        Variable.set("atg_dbt_last_processed_at", source_watermark)
        return result

    watermark = detect_completed_atg_load()
    built_watermark = run_dbt_build(watermark)
    validate_dbt_output(built_watermark)


dag_atg_dbt()
