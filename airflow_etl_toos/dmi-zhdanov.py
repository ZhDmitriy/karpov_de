"""
Тестовый даг из трех тасок:
1. DummyOperator
2. BashOperator: вывод даты
3. PythonOperator: вывод даты
"""

# импортируем нужные библиотеки
from airflow import DAG  #объект airflow
from airflow.utils.dates import datetime
from datetime import timedelta
import logging

from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# словарь данных
DEFAULT_ARGS = {
    'start_date': datetime(2024, 7, 22),
    'end_date': datetime(2025, 7,22),
    'owner': 'dmi-zhdanov',
    'retries': 3,
    'poke_interval': 600
}

with DAG("dmi-zhdanov_first_dag",
          schedule_interval='@daily',
          default_args=DEFAULT_ARGS,
          max_active_tasks=1,
          tags=['zhdanov_student']
        ) as dag:

    # сенсор
    wait_start_tasks = TimeDeltaSensor(
        task_id="wait_start_tasks",
        delta=timedelta(6*60*60),
        dag=dag
    )

    # Task 1 = DummyOperator
    dummy_task = DummyOperator(
        task_id='dummy_task',
        dag=dag
    )

    # Task 2 = BashOperator
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ds }}',
        dag=dag
    )

    # Task 3 = PythonOperator
    def print_now_date():
        return datetime.now()


    python_task = PythonOperator(
        task_id='python_task',
        python_callable=print_now_date,
        dag=dag
    )

    wait_start_tasks >> dummy_task >> bash_task >> python_task




