# Выполняем map-reduce задание локально

""" mapper.py """

import pandas as pd
from tqdm import tqdm
import datetime
from downloader_s3 import (
    S3_YANDEX_OBJECT_STORAGE_KEY,
    S3_YANDEX_OBJECT_STORAGE_SECRET_KEY,
    S3_YANDEX_OBJECT_STORAGE_ENDPOINT
)
import boto3 # стандартная библиотека для работы с хранилищем S3


def get_s3_instance():
    """ Создаем клиента S3 """
    s3 = boto3.Session(
        aws_access_key_id=S3_YANDEX_OBJECT_STORAGE_KEY,
        aws_secret_access_key=S3_YANDEX_OBJECT_STORAGE_SECRET_KEY).client(
            service_name='s3', endpoint_url=S3_YANDEX_OBJECT_STORAGE_ENDPOINT
    )
    return s3


def get_s3_backet():
    """ Забираем загруженный файл в S3 бакет, размещенный на Yandex.Cloud  """
    return get_s3_instance().list_objects(Bucket='mapreduce')['Contents']


def perform_map(data: pd.DataFrame):
    """
        (Месяц, (Payment type, Tips average amount)) - возвращается значение следующего типа
    """
    result_list = []
    mapping = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip'
    }
    for item in tqdm(range(len(data))):
        year = datetime.datetime.strptime(str(data.iloc[item]['tpep_pickup_datetime']), '%Y-%m-%d %H:%M:%S').year
        if str(year) == '2020':
            month = datetime.datetime.strptime(str(data.iloc[item]['tpep_pickup_datetime']), '%Y-%m-%d %H:%M:%S').month # Получаем месяц строки
            try:
                payment_type = mapping[int(data.iloc[item]['payment_type'])] # Получаем тип оплаты
            except:
                payment_type = "None"
            tip_amount = data.iloc[item]['tip_amount'] # Стоимость поездки
            result_list.append(
                (month, (payment_type, tip_amount))
            )
        return result_list


if __name__ == '__main__':
    key = get_s3_backet()[0]['Key']
    fileObj = get_s3_instance().get_object(Bucket='mapreduce', Key=key)
    fileContent = pd.read_csv(fileObj['Body'])
    #csv = pd.read_csv(r'D:\mapreduce\yellow_tripdata_2020-01.csv').fillna(0)
    reducer_continue = perform_map(fileContent)
    df = pd.DataFrame(reducer_continue, columns=['month', 'value'])
    df[['payment_type', 'tip_amount']] = pd.DataFrame(df['value'].tolist(), index=df.index)
    result_df = df[['month', 'payment_type', 'tip_amount']]
    result_df.to_csv('reducer.csv')
