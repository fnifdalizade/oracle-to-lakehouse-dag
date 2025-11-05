# dags/oracle_to_lakehouse_kpi.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pyhive import hive
import cx_Oracle
import requests
from airflow.hooks.base import BaseHook
from pendulum import timezone

local_tz = timezone("Asia/Baku")

# ===============================
# Messaging (Telegram) – DUMMY VALUES FOR PORTFOLIO
# ===============================
TELEGRAM_TOKEN = "123"   # <- dummy
CHAT_ID = 123            # <- dummy

def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload)
    except Exception as e:
        print(f"Telegram error: {e}")

# ===============================
# Callbacks
# ===============================
def notify_failure(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    message = f"❌ DAG: {dag_id}, Task: {task_id} FAILED on {execution_date}"
    send_telegram_message(message)

# (Optional success callback kept commented as in original)
# def notify_success(context):
#     dag_id = context.get('dag').dag_id
#     task_id = context.get('task_instance').task_id
#     execution_date = context.get('execution_date')
#     message = f"✅ DAG: {dag_id}, Task: {task_id} SUCCEEDED on {execution_date}"
#     send_telegram_message(message)

# ===============================
# Connections (neutral IDs)
# ===============================
src_conn            = BaseHook.get_connection('source_oracle')   # was 'oracle11g'
src_user_name       = src_conn.login
src_user_pass       = src_conn.password
src_host            = src_conn.host
src_service         = src_conn.schema
src_port            = src_conn.port

dst_conn            = BaseHook.get_connection('lakehouse_sql')   # was 'iomete'
dst_user_name       = dst_conn.login
dst_user_pass       = dst_conn.password
dst_host            = dst_conn.host
dst_database        = dst_conn.schema
dst_port            = dst_conn.port

# ===============================
# Default args
# ===============================
default_args = {
    "owner": "data_team",
    "start_date": datetime(2024, 1, 1, tzinfo=local_tz),
    "on_failure_callback": notify_failure,
    # "on_success_callback": notify_success,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ===============================
# ETL task
# ===============================
def oracle_to_lakehouse():
    # ---- Oracle connect
    dsn = cx_Oracle.makedsn(
        host=src_host,
        port=src_port,
        service_name=src_service
    )
    oracle_conn = cx_Oracle.connect(
        user=src_user_name,
        password=src_user_pass,
        dsn=dsn
    )

    # ---- Business SQL (anonymized objects, same shape/aliases)
    oracle_sql = """
        /* Aggregated example query (objects anonymized) */
        SELECT
            TRUNC(t.event_dt) AS tarix,
            t.service_code    AS service_code,
            t.description     AS description,
            SUM(t.payed)      AS payed_amount
        FROM analytics_src t
        WHERE t.status > 0
          AND t.event_dt >= TO_DATE('01.01.2024 00:00:00','dd.mm.yyyy hh24:mi:ss')
          AND EXISTS (
              SELECT 1
              FROM trx_log tl
              WHERE tl.declaration_id = t.declaration_id
          )
        GROUP BY TRUNC(t.event_dt), t.service_code, t.description
        ORDER BY 1
    """

    oracle_df = pd.read_sql(oracle_sql, oracle_conn)
    oracle_df = oracle_df.replace({np.nan: None, 'nan': None, "'nan'": None})

    data_tuples = [
        tuple(None if pd.isna(x) else x for x in oracle_df.to_numpy()[i])
        for i in range(len(oracle_df))
    ]

    # ---- Lakehouse (Hive/Trino-compatible)
    hive_conn = hive.connect(
        host=dst_host,
        port=dst_port,
        scheme="https",
        data_plane="default-plane",        # anonymized
        lakehouse="default-compute",       # anonymized
        database=dst_database,             # neutralized
        username=dst_user_name,
        password=dst_user_pass
    )
    hive_cursor = hive_conn.cursor()

    # target table anonymized: was spark_catalog.dashboards.DASH_FORMA_3_6_22
    target_table = "spark_catalog.analytics.kpi_daily"

    # ---- Idempotent overwrite pattern
    del_query = f"DELETE FROM {target_table}"
    hive_cursor.execute(del_query)
    hive_conn.commit()

    def generate_multi_insert(rows, columns):
        base_query = f"INSERT INTO {target_table} ({','.join(columns)}) VALUES "
        value_groups = []

        for row in rows:
            values = []
            for val in row:
                if val is None or val in ['nan', "'nan'"]:
                    values.append("NULL")
                elif isinstance(val, str):
                    values.append(f"""'{val.replace("'", "''")}'""")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, bool):
                    values.append("1" if val else "0")
                else:
                    values.append(f"'{str(val)}'")
            value_groups.append(f"({','.join(values)})")

        return base_query + ','.join(value_groups)

    batch_size = 1000
    for i in range(0, len(data_tuples), batch_size):
        batch = data_tuples[i:i + batch_size]
        try:
            query = generate_multi_insert(batch, oracle_df.columns)
            hive_cursor.execute(query)
            hive_conn.commit()
        except Exception as e:
            print(f"Error inserting batch {i//batch_size}: {str(e)}")
            hive_conn.rollback()

    hive_cursor.close()
    hive_conn.close()
    oracle_conn.close()

# ===============================
# DAG (names/tags anonymized: no "dgk", no "forma 3/6/12")
# ===============================
with DAG(
    dag_id="oracle_to_lakehouse_kpi",   # was oracle_to_iomete_3_6_22
    default_args=default_args,
    description="Transfers aggregated data from Oracle to Lakehouse every 2 hours (07:00-21:00).",
    schedule_interval="0 7-21/2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "kpi_pipeline"],  # was ["dgk_dashboard","forma_3_6_22"]
) as dag:

    run_etl_task = PythonOperator(
        task_id="oracle_to_lakehouse_transfer",  # kept neutral
        python_callable=oracle_to_lakehouse
    )
