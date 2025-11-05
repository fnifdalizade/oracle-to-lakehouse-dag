# Oracle → Lakehouse ETL with Airflow

This project demonstrates a production-style ETL pipeline built with **Apache Airflow**, transferring aggregated data from an **Oracle database** to a **Lakehouse (Hive/Trino-compatible)** destination. The pipeline runs every 2 hours between 07:00 and 21:00 and includes built-in failure alerts through Telegram.

---

## 🧠 Overview

The DAG connects to an Oracle source, runs a business aggregation query, cleans up the dataset, and loads it into a Lakehouse table. It overwrites the existing data safely using batched inserts, commits per batch, and rollback on error. All credentials and tokens are stored securely in **Airflow Connections** or environment variables.

---

## ⚙️ Key Features

* **Automated 2-hour schedule** (`0 7-21/2 * * *`)
* **Oracle → Lakehouse ETL** with `cx_Oracle` and `pyhive`
* **Batched Inserts** for efficient loading (~1000 rows per batch)
* **Data Cleaning:** replaces `NaN`, `'nan'`, and null strings with `NULL`
* **Error Handling:** rollback on failure and Telegram notifications
* **Environment-agnostic setup:** all secrets in Airflow Connections
* **Timezone aware:** uses `Asia/Baku` via `pendulum`

---

## 📦 Project Structure

```
dags/
 └── oracle_to_lakehouse.py
```

---

## 🔑 Airflow Connections

| Conn ID         | Purpose                       | Required Fields                                          |
| --------------- | ----------------------------- | -------------------------------------------------------- |
| `src_oracle`    | Source Oracle Database        | Host, Port, Schema, Login, Password                      |
| `dst_lakehouse` | Target Lakehouse (Hive/Trino) | Host, Port, Schema, Login, Password                      |
| `telegram_bot`  | Telegram Notification Bot     | Password (Bot Token), Extra → `{"chat_id": "<CHAT_ID>"}` |

---

## 🔔 Notifications

* On **task failure**, a message is sent to a Telegram chat:

  ```
  ❌ DAG: oracle_to_lakehouse_kpi • Task: oracle_to_lakehouse_transfer failed at <timestamp>
  ```
* Implemented via a callback `on_failure_callback=notify_failure`.

---

## How It Works

1. **Extract:** Executes the Oracle SQL to pull summarized business data.
2. **Transform:** Cleans and prepares the DataFrame (handles missing values).
3. **Load:** Deletes existing table data and inserts new batches into the Lakehouse.
4. **Notify:** Sends a Telegram message if any task fails.

---

## Dependencies

```
apache-airflow
cx_Oracle
pyhive
pandas
numpy
pendulum
requests
```

---

## Example Use Case

Ideal for dashboards or analytical pipelines where:

* Data comes from an operational RDBMS (e.g., Oracle)
* Needs to be periodically synchronized to a central Lakehouse
* Monitoring and alerting are required for reliability

---

## Author

**Fakhri Nifdalizadeh**
Data Engineer & Analyst
[LinkedIn](https://linkedin.com/in/fakhrinifdalizadeh) · [Email](mailto:fnifdalizada@gmail.com)

---
