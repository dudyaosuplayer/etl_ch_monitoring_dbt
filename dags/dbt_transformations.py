from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="dbt_transformations",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
        mkdir -p /tmp/dbt_target /tmp/dbt_logs &&

        cd /opt/dbt &&

        dbt build \
          --profiles-dir /home/airflow/.dbt \
          --target-path /tmp/dbt_target \
          --log-path /tmp/dbt_logs
        """
    )