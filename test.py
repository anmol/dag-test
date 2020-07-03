from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def simple_task(task_id):
    print(task_id)


task_1 = PythonOperator(
    task_id='task_1',
    python_callable=simple_task,
    op_kwargs={'task_id': task_id},
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=simple_task,
    op_kwargs={'task_id': task_id},
    dag=dag
)

task_2.set_upstream(task_1)
