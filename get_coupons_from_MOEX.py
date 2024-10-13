import pandas as pd

import requests

import datetime

import xml.etree.ElementTree as ET

import sqlite3

"""Написали базовый код для получения списка купонов по облигациям и 
сохраниения значений в базу SQL с пагинацией и с рекурсией"""

"""Ссылка на видео"""
"""https://www.youtube.com/watch?v=xBADEZK0TwI"""

"""Ссылка на статью к видео"""
"""https://алготрейдинг.рф/moex/iss-moex/"""

conn = sqlite3.connect('tinkoffAPIdata.db')
tickers = list(pd.read_sql_query('SELECT ticker FROM API_T_specs', conn)['ticker'])


def get_coupons(conn, secid: str, start=0, limit=50):
    """
    Функция для получения списка купонов по облигации и сохранния данных в SQL
    С ПАГИНАЦИЕЙ
    :param i:
    :param secid:
    :return:
    """

    dfs = {}

    while True:
        url = f'https://iss.moex.com/iss/securities/{secid}/bondization.xml'

        params = {
            'limit': limit,
            'start': start
        }

        response = requests.get(url=url, params=params)

        root = ET.fromstring(response.content)

        rows = root.findall("./data/rows/row")

        if not rows:
            return save_coupons(conn, dfs, secid)

        start += limit

        for data in root.findall('./data'):
            id = data.attrib['id']

            columns = []
            for c in data.findall(".metadata/columns/column"):
                columns.append(c.attrib['name'])

            df = pd.DataFrame(columns=columns)

            rows = data.findall('./rows/row')

            for row in rows:
                data = dict(zip(columns, [row.attrib.get(c) for c in columns]))
                df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)

            if not id in dfs:
                dfs[id] = df
            else:
                dfs[id] = pd.concat([dfs[id], df], ignore_index=True)




def save_coupons(conn, dfs: pd.DataFrame, secid):
    for id, df in dfs.items():
        df['update_time'] = datetime.datetime.now()
        df.to_sql(f'{secid}_{id}', conn, if_exists="replace", index=False)

        if "id" in df.columns:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA index_info('index_name')")
            index_info = cursor.fetchall()

            if not index_info:
                cursor.execute(f"CREATE INDEX index_name ON {id} (id)")
                conn.commit()

for ticker in tickers:
    get_coupons(conn, ticker, 0, 50)


conn.close()
print('Данные успешно записаны')
