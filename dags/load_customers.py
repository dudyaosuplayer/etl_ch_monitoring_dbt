from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from clickhouse_driver import Client


def load_csv_to_clickhouse():
    df = pd.read_csv("/opt/airflow/data/customers.csv")

    # переименование колонок
    df.columns = [
        "idx",
        "customer_id",
        "first_name",
        "last_name",
        "company",
        "city",
        "country",
        "phone_1",
        "phone_2",
        "email",
        "subscription_date",
        "website"
    ]

    df["subscription_date"] = pd.to_datetime(
        df["subscription_date"]
    ).dt.date

    client = Client(host="clickhouse")
    # создание DB
    client.execute("CREATE DATABASE IF NOT EXISTS raw")
    # создание таблицы
    client.execute("""
        CREATE TABLE IF NOT EXISTS raw.customers (
            idx UInt32,
            customer_id String,
            first_name String,
            last_name String,
            company String,
            city String,
            country String,
            phone_1 String,
            phone_2 String,
            email String,
            subscription_date Date,
            website String
        )
        ENGINE = MergeTree()
        ORDER BY idx
    """)

    # очистка
    client.execute("TRUNCATE TABLE raw.customers")

    # загрузка
    client.execute(
        "INSERT INTO raw.customers VALUES",
        df.to_dict("records")
    )

    client.execute("TRUNCATE TABLE raw.customers")

    client.execute(
        "INSERT INTO raw.customers VALUES",
        df.to_dict("records")
    )


with DAG(
    dag_id="load_customers_csv",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv_to_clickhouse
    )