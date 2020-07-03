from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 7, 3),
    'email': ['anmol.gautam@traveloka.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'dag-test',
    default_args=default_args,
    schedule_interval='*/10 * * * *'  # every 10 mins
)


def simple_task(task_id):
    print(task_id)


task_1 = PythonOperator(
    task_id='task_1',
    python_callable=simple_task,
    op_kwargs={'task_id': 1},
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=simple_task,
    op_kwargs={'task_id': 2},
    dag=dag
)

task_2.set_upstream(task_1)
