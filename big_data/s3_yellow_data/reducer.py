# Выполняем map-reduce задание локально

""" reducer.py """

import pandas as pd


def perform_reduce(file_reducer: pd.DataFrame) -> pd.DataFrame:
    file_reducer = file_reducer[['month', 'payment_type', 'tip_amount']]
    file_reducer = file_reducer.groupby(['month', 'payment_type']).agg({'tip_amount': 'mean'})
    file_reducer.rename(columns={"month": "month", "payment_type": "payment_type", "tip_amount": "tips average amount"})
    return file_reducer


if __name__ == '__main__':
    perform_reduce(pd.read_csv(r"D:\mapreduce\reducer.csv")).to_csv('result.csv')