# oracle-to-lakehouse-dag
This project demonstrates a production-style ETL pipeline built with Apache Airflow, transferring aggregated data from an Oracle database to a Lakehouse (Hive/Trino-compatible) destination. The pipeline runs every 2 hours between 07:00 and 21:00 and includes built-in failure alerts through Telegram.
