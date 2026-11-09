import os
from datetime import datetime, timezone

import requests
from pymongo import MongoClient, UpdateOne
from airflow.sdk import dag, task  

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE = "EUR"
QUOTES = "USD,GBP,JPY"


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency"],
)
def currency_exchange_etl():

    @task
    def extract(ti=None):
        resp = requests.get(API_URL, params={"base": BASE, "quotes": QUOTES}, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        ti.xcom_push(key="raw_rates", value=raw)          # named XCom

    @task
    def transform(ti=None):
        raw = ti.xcom_pull(task_ids="extract", key="raw_rates")
        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        docs = [
            {
                "base_currency": row["base"],
                "target_currency": row["quote"],
                "exchange_rate": row["rate"],
                "rate_date": row["date"],
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            for row in raw
        ]
        ti.xcom_push(key="transformed_rates", value=docs)  # named XCom

    @task
    def load(ti=None):
        docs = ti.xcom_pull(task_ids="transform", key="transformed_rates")
        client = MongoClient(os.environ["MONGO_URI"])
        col = client["currency_db"]["exchange_rates"]
        ops = [
            UpdateOne(
                {"base_currency": d["base_currency"],
                 "target_currency": d["target_currency"],
                 "rate_date": d["rate_date"]},
                {"$set": d},
                upsert=True,          # no duplicates if DAG re-runs
            )
            for d in docs
        ]
        result = col.bulk_write(ops)
        print(f"Upserted {result.upserted_count}, modified {result.modified_count}")
        client.close()

    extract() >> transform() >> load()


currency_exchange_etl()