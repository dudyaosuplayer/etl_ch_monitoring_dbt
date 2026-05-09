from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


with DAG(
    dag_id="full_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    load_customers = TriggerDagRunOperator(
        task_id="trigger_load_customers",
        trigger_dag_id="load_customers_csv",
        wait_for_completion=True,
        poke_interval=10,
    )

    load_orders = TriggerDagRunOperator(
        task_id="trigger_load_orders",
        trigger_dag_id="load_orders_csv",
        wait_for_completion=True,
        poke_interval=10,
    )

    dbt_transformations = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="dbt_transformations",
        wait_for_completion=True,
        poke_interval=10,
    )

    [load_customers, load_orders] >> dbt_transformations