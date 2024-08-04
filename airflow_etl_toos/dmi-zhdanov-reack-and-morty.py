"""
    Обрабатываем, анализируем и кладем в GreenPlum данные с сериала Reack And Morty о локациях
"""

from airflow import DAG  #объект airflow
from airflow.utils.dates import datetime
from datetime import timedelta
from airflow.models.xcom import XCom
import logging  #логирование внутри AirFlow
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator  #настраиваем оператор ветвления между тасками
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook  #импортируем хук/подключение, которое настраивается в AirFlow
import requests
import pandas as pd
import collections
import json
from airflow.exceptions import AirflowException

# словарь данных
DEFAULT_ARGS = {
    'start_date': datetime(2024, 8, 4), #дата начала работа дага
    'end_date': datetime(2025, 8, 4), #дата окончания работы дага
    'owner': 'dmi-zhdanov', #автор
    'retries': 3, #количество попыток перезапуска в случае падения одного из тасков
    #'retry_delay': timedelta(minutes=3), #задержка между перезапусками тасков
    'email': ['zhdanovwork@yandex.ru'], #отправляем сообщение на email, если
    'email_on_failure': True, #какой-либо из тасков упал
    'poke_interval': 600 #время между попытками перезапуска, сколько ожидать
}

@dag('dmi-zhdanov-reack-and-morty', schedule_interval='@daily', default_args=DEFAULT_ARGS, max_active_tasks=1, tags=['zhdanov_student']) #количество задач, которое может выполняться одновременно для данного таска
def taskflow():

    @task()
    def create_table_in_greenplum():
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(''' 
                create table dmi_zhdanov_ram_location(
                    id serial primary key, 
                    name varchar(200),
                    type varchar(200), 
                    dimension varchar(200),
                    resident_cnt varchar(200)
                )
                ''')
            conn.commit()
            logging.info("Таблица успешно создана в GreenPlum")
            return "read_api_reack_and_morty"
        except Exception as error:
            logging.error(f"Таблица уже создана в GreenPlum, код ошибки {error}")
            return "read_api_reack_and_morty"

    @task()
    def read_api_reack_and_morty():
        def get_values_page(page: json) -> pd.DataFrame:
            df_ricky = collections.defaultdict(list)
            for item in page['results']:
                for item_residents in item['residents']:
                    df_ricky['id'].append(item['id'])
                    df_ricky['name'].append(item['name'])
                    df_ricky['type_name'].append(item['type'])
                    df_ricky['dimension'].append(item['dimension'])
                    df_ricky['residents'].append(item_residents)
                return pd.DataFrame({
                    "id": df_ricky['id'],
                    "name": df_ricky['name'],
                    "type_name": df_ricky['type_name'],
                    "dimension": df_ricky['dimension'],
                    "residents": df_ricky['residents']
                })
        if requests.get('https://rickandmortyapi.com/api/location').status_code == 200:
            base_url = 'https://rickandmortyapi.com/api/location'
            page = requests.get(url=base_url).json() #отправляем первый запрос
            df_rickymorty = get_values_page(page=page)
            next_pg = page['info']['next'] #следующий url для запроса
            count_rows = page['info']['count']
            count = len(page['results'])
            while count != count_rows:
                next_page = requests.get(url=next_pg).json() #следующая страница
                df_rickymorty = pd.concat([df_rickymorty, get_values_page(page=next_page)], ignore_index=True)
                count += len(next_page['results'])
                next_pg = next_page['info']['next']
        else:
            raise AirflowException("Error requests read location Reack And Morty API")
        logging.info(f"Кол-во ответов в запросе = {count_rows}")
        logging.info(f"Кол-во собранных значений = {count}")
        return df_rickymorty

    @task()
    def analazy_response_from_api(rick_and_morty_api_response: pd.DataFrame):
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        rick_and_morty_api_response = rick_and_morty_api_response.groupby(by=["id", "name", "type_name", "dimension"], dropna=False).count().reset_index().sort_values(by='residents', ascending=False).iloc[:3]
        cursor.execute(f'''SELECT * FROM dmi_zhdanov_ram_location''')
        query_res = cursor.fetchall()
        if len(list(query_res)) != 0:
            #пробегаемся по новым значениям, если нашли имя, которое уже есть, то перезаписываем всю строку, которая принадлежит этому имени
            for item_row in range(len(rick_and_morty_api_response)):
                if rick_and_morty_api_response.iloc[item_row]['name'] in list(query_res['name']):
                    cursor.execute(f'''
                        UPDATE dmi_zhdanov_ram_location 
                        SET 
                            name = "{rick_and_morty_api_response.iloc[item_row]['name']}", 
                            type = "{rick_and_morty_api_response.iloc[item_row]['type_name']}", 
                            dimension = "{rick_and_morty_api_response.iloc[item_row]['dimension']}", 
                            resident_cnt = "{rick_and_morty_api_response.iloc[item_row]['residents']}"
                        ''')
                    conn.commit()
            return str('update_data_success')
        else:
            return rick_and_morty_api_response

    @task()
    def load_new_data_from_api_reack_and_morty(df_reack_and_morty_for_load) -> None:
        pg_hook = PostgresHook(postgres_conn_id='conn_greenplum_write')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        try:
            if df_reack_and_morty_for_load == 'update_data_success':
                logging.info("Данные успешно обновлены")
        except:
            logging.info("Данных для обновления нет")
        for item in range(len(df_reack_and_morty_for_load)):
            cursor.execute(f"""
                INSERT INTO dmi_zhdanov_ram_location (name, type, dimension, resident_cnt)
                VALUES (
                        '{str(df_reack_and_morty_for_load.iloc[item]['name'])}',
                        '{str(df_reack_and_morty_for_load.iloc[item]['type_name'])}', 
                        '{str(df_reack_and_morty_for_load.iloc[item]['dimension'])}', 
                        '{str(df_reack_and_morty_for_load.iloc[item]['residents'])}')
                """)
            conn.commit()
            logging.info("New data success load to GreenPlum")

    create_table_greenplum = create_table_in_greenplum()
    requests_api_reack_and_morty = read_api_reack_and_morty()
    analazy_response = analazy_response_from_api(rick_and_morty_api_response=requests_api_reack_and_morty)
    load_new_data = load_new_data_from_api_reack_and_morty(df_reack_and_morty_for_load=analazy_response)

    create_table_greenplum >> requests_api_reack_and_morty >> analazy_response >> load_new_data # цепочка задач

dmi_zhdanov_reack_and_morty_dag = taskflow()