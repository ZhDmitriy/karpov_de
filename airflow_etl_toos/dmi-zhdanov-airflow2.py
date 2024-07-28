"""
Даг из четырех тасок:
1. DummyOperator
2. BashOperator: вывод даты
3. PythonOperator: вывод даты
4. Ходим в GreenPlum забираем данные из таблицы articles с наложенными условиями

Даг отрабатывается с понедельника по субботу
"""

# импортируем нужные библиотеки
from airflow import DAG  # объект airflow
from airflow.utils.dates import datetime
from datetime import timedelta
from airflow.models.xcom import XCom
import logging  # логгирование внутри AirFlow
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.bash import BashOperator

from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator, BranchPythonOperator  # настраиваем оператор ветвления между тасками
from airflow.hooks.postgres_hook import PostgresHook  # импортируем хук/подключение, которое настраивается в AirFlow

# словарь данных
DEFAULT_ARGS = {
    'start_date': datetime(2022, 3, 1),  # дата начала работа дага
    'end_date': datetime(2022, 3, 14),  # дата окончания работы дага
    'owner': 'dmi-zhdanov',  # автор
    'retries': 3,  # количество попыток перезапуска
    'poke_interval': 600  # время между попытками перезапуска, сколько ожидать
}

with DAG('dmi-zhdanov-airflow2',
         schedule_interval='@daily',  # выполняем даг каждый день, но с условиями, прописанными позже
         default_args=DEFAULT_ARGS,
         max_active_tasks=1,
         tags=['zhdanov_student']
         ) as dag:

    def start_tasks_next(execution_dt) -> bool:
        """
            0 - понедельник
            1 - вторник
            2 - среда
            3 - четверг
            4 - пятница
            5 - суббота
            6 - воскресенье
        """
        exec_day = datetime.strptime(execution_dt, '%Y-%m-%d').weekday()  # возвращаем номер дня недели
        if exec_day in [0, 1, 2, 3, 4, 5]:
            return 'output_greenplum_task'
        else:
            return 'task_end'

    get_day_of_week = BranchPythonOperator(
        task_id='get_day_of_week_branch',
        python_callable=start_tasks_next,
        op_kwargs={'execution_dt': '{{ ds }}'} # время на момент выполнения дага, а не текущее
    )

    end_task = DummyOperator(
        task_id="task_end"
    )

    # Ходим в GreenPlum и сохраняем результаты в Xcom AirFlow
    def get_data_greenplum(day_week: int):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f'''SELECT heading FROM articles WHERE id = {day_week}''')
        query_res = cursor.fetchall()
        logging.info(query_res)
        return query_res

    # Получаем вывод результата в логах AirFlow
    output_greenplum = PythonOperator(
        task_id='output_greenplum_task',
        python_callable=get_data_greenplum,
        op_args=[datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d').weekday()],
        dag=dag
    )

    get_day_of_week >> [output_greenplum, end_task]
