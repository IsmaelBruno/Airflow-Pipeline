# Airflow Data Platform

Airflow runs two independent workflows:

- `dag_excel_ingestion` runs every five minutes: it scans the configured network
  source, waits for files to stabilize, copies and verifies them through the
  existing L–D script, then runs the existing Excel pipeline and checks its
  PostgreSQL audit table.
- `dag_atg_dbt` runs every five minutes. It does not launch or control NiFi.
  It watches successful entries in `public.file_audit`, waits for a quiet
  interval, runs `dbt build` with the `dev` profile, validates
  `dbt_dev_prod.tank_measurements`, and advances an Airflow watermark only after
  validation succeeds.

The ATG workflow observes successful database loads, not NiFi's internal
processor state. A NiFi failure that produces no database audit entry is
therefore not reported as a NiFi failure; direct NiFi API monitoring requires
an authenticated API endpoint and a stable process-group identifier.

## Build and start

From `D:\Airflow`:

```powershell
docker compose build airflow-worker
docker compose up -d airflow-worker airflow-scheduler airflow-dag-processor
docker compose exec -T airflow-scheduler airflow dags list
```

The first DAG runs process all eligible Excel files already present in
`D:\SBM\BI_SBM\Entrée` and `D:\SBM\BI_SBM\Sortie`; matching content already
loaded successfully is skipped by the existing pipeline.

After confirming the DAGs and mounts, enable them:

```powershell
docker compose exec -T airflow-scheduler airflow dags unpause -y dag_excel_ingestion
docker compose exec -T airflow-scheduler airflow dags unpause -y dag_atg_dbt
```

The copy source and destination are mounted in the worker at `/mnt/bi-source`
and `/mnt/bi-dest`. The Excel project `.env` and dbt project `.env` remain
outside the Airflow image and are mounted read-only with their projects. The
worker reads their configuration at task runtime; secrets must not be copied
into DAG source or image build arguments.

## Operations

```powershell
docker compose logs --tail 100 airflow-worker
docker compose exec -T airflow-scheduler airflow dags list
```

For run history, task states, and per-task logs, use the Airflow UI at
`http://localhost:8081`.

Excel retries are safe because the copy is checksum-verified and the ingestion
records successful file hashes. ATG/dbt retries are safe because dbt is
re-runnable and the watermark advances only after output validation. Airflow
task logs and run states are the operational record; no external email or chat
alerting is configured by this local Compose setup.
