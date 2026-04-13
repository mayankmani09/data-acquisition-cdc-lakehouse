from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="data_acquisition_cdc_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "cdc", "lakehouse"],
) as dag:
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python -m src.main",
    )

    run_tests = BashOperator(
        task_id="run_tests",
        bash_command="pytest tests/unit -q",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="cd dbt && dbt build --profiles-dir .",
    )

    generate_data >> run_tests >> run_dbt
