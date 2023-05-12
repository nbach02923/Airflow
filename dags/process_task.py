from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry-delay": timedelta(seconds=5),
    "depends_on_past": False,
}
dag = DAG(
    dag_id="process_task",
    default_args=default_args,
    # start_date = datetime(2023, 1, 1, 0, 0, 0, 0),
    start_date=datetime.today(),
    schedule_interval="0 2 * * *",
)
process_csv = SparkSubmitOperator(
    task_id="spark_process_csv",
    dag=dag,
    conn_id="spark",
    application="/opt/airflow/dags/script/spark_csv.py",
)
process_csv
