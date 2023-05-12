from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from script.crawl_script import crawling
default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry-delay' : timedelta(seconds=5),
    'depends_on_past': False,
}
dag =  DAG(
    dag_id = 'crawl_task',
    default_args = default_args,
    start_date = datetime.today(),
    schedule_interval = '0 0 * * *'
)
insert_data = PythonOperator(
    task_id = 'write_data_file',
    dag = dag,
    python_callable = crawling,
)