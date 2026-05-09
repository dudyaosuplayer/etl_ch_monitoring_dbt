from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from faker import Faker

import random
import pandas as pd

from clickhouse_driver import Client


fake = Faker()


PRODUCTS = {
    "Electronics": [
        "iPhone",
        "Laptop",
        "Headphones",
        "Monitor",
        "Keyboard"
    ],
    "Clothing": [
        "T-Shirt",
        "Jeans",
        "Jacket",
        "Sneakers"
    ],
    "Books": [
        "Python Book",
        "SQL Guide",
        "Data Engineering",
        "Machine Learning"
    ]
}


STATUSES = [
    "created",
    "paid",
    "shipped",
    "delivered",
    "cancelled"
]


def generate_orders():

    customers = pd.read_csv(
        "/opt/airflow/data/customers.csv"
    )

    customer_ids = customers["Customer Id"].tolist()

    rows = []

    for order_id in range(1, 10001):

        category = random.choice(list(PRODUCTS.keys()))
        product = random.choice(PRODUCTS[category])

        rows.append({
            "order_id": order_id,
            "customer_id": random.choice(customer_ids),
            "order_date": fake.date_between(
                start_date="-2y",
                end_date="today"
            ),
            "product": product,
            "category": category,
            "quantity": random.randint(1, 5),
            "amount": round(random.uniform(10, 3000), 2),
            "status": random.choice(STATUSES)
        })

    df = pd.DataFrame(rows)

    df.to_csv(
        "/opt/airflow/data/orders.csv",
        index=False
    )


def load_orders_to_clickhouse():

    df = pd.read_csv("/opt/airflow/data/orders.csv")

    df["order_date"] = pd.to_datetime(
        df["order_date"]
    ).dt.date

    client = Client(host="clickhouse")

    client.execute("""
        CREATE DATABASE IF NOT EXISTS raw
    """)

    client.execute("""
        CREATE TABLE IF NOT EXISTS raw.orders (
            order_id UInt32,
            customer_id String,
            order_date Date,
            product String,
            category String,
            quantity UInt32,
            amount Float64,
            status String
        )
        ENGINE = MergeTree()
        ORDER BY order_id
    """)

    client.execute("""
        TRUNCATE TABLE raw.orders
    """)

    client.execute(
        "INSERT INTO raw.orders VALUES",
        df.to_dict("records")
    )


with DAG(
    dag_id="load_orders_csv",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    generate_csv = PythonOperator(
        task_id="generate_orders_csv",
        python_callable=generate_orders
    )

    load_orders= PythonOperator(
        task_id="load_orders",
        python_callable=load_orders_to_clickhouse
    )


    generate_csv >> load_orders