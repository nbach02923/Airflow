from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator
from datetime import timedelta, datetime
from airflow import DAG

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry-delay": timedelta(seconds=5),
    "depends_on_past": False,
}
dag = DAG(
    dag_id="upload",
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval="0 0 * * *",
)
upload_task = LocalFilesystemToGoogleDriveOperator(
    task_id = "upload_to_drive",
    gcp_conn_id = ""
)