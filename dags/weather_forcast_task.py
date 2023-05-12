from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from script.open_meteo_fetch import fetch_api
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry-delay": timedelta(seconds=5),
    "depends_on_past": False,
}
dag = DAG(
    dag_id="weather_forecast",
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval="0 0 * * 1",
)
weather_forecast = PythonOperator(
    task_id="weather_forecast",
    dag=dag,
    python_callable=fetch_api
)
