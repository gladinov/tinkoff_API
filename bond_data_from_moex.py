import pandas as pd

import sqlite3

import requests

from urllib import parse

import datetime

from save_df_to_sql import df_to_sql

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

"""1.Открываем файл,который получаем в программе portfolio_v1.py """
conn = sqlite3.connect("tinkoffAPIdata.db")
df = pd.read_sql_query("SELECT * FROM API_T_specs", conn)


def query(method: str, **kwargs):
    """
    Отправляю запрос к ISS MOEX
    :param method:
    :param kwargs:
    :return:
    """
    try:
        url = "https://iss.moex.com/iss/%s.json" % method
        if kwargs:
            if '_from' in kwargs: kwargs['from'] = kwargs.pop(
                '_from')  # костыль - from нельзя указывать как аргумент фн, но в iss оно часто исп
            url += "?" + parse.urlencode(kwargs)

        # не обязательно
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36', }

        r = requests.get(url, headers=headers)
        r.encoding = 'utf-8'
        j = r.json()
        return j

    except Exception as e:
        print("query error %s" % str(e))
        return None


def flatten(j: dict, blockname: str):
    """
    Собираю двумерный массив (словарь)
    :param j:
    :param blockname:
    :return:
    """
    return [{k: r[i] for i, k in enumerate(j[blockname]['columns'])}
            for r in j[blockname]['data']]


def rows_to_dict(j: dict, blockname: str, field_key='name', field_value='value'):
    """
    Для преобразования запросов типа /securities/:secid.json (спецификация бумаги)
    в словарь значений
    :param j:
    :param blockname:
    :param field_key:
    :param field_value:
    :return:
    """
    return {str.lower(r[field_key]): r[field_value] for r in flatten(j, blockname)}


def get_specs(secid: str):
    return rows_to_dict(query(f"securities/{secid}"), 'description')


def get_yield(secid: str):
    path = f"history/engines/stock/markets/bonds/sessions/3/securities/{secid}"
    _from = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    _r = flatten(query(path, _from=_from), 'history')

    # если сделок не было, то что-то нужно записать в базу чтобы не запрашивать облигу сегодня ещё
    # todo: проверить - гипотетически пустые ответы могут быть сбоем
    if len(_r) < 1: return {'price': 0, 'yieldsec': 0, 'tradedate': datetime.datetime.now().strftime("%Y-%m-%d"),
                            'volume': 0}

    return {
        'secid': secid,
        'price': _r[-1]['CLOSE'],
        'yieldsec': _r[-1]['YIELDCLOSE'],
        'tradedate': _r[-1]['TRADEDATE'],
        'volume': _r[-1]['VOLUME'],
    }


def get_duration(secid: str, class_code: str):
    path = f"engines/stock/markets/bonds/boards/{class_code}/securities/{secid}"
    return {
        'secid': secid,
        'DURATION': flatten(query(path, ), 'marketdata_yields')[0]['DURATION']
    }


def report(df: pd.DataFrame):
    ticker_list = list(df['ticker'])
    df_specs = pd.DataFrame([get_specs(i) for i in ticker_list])
    df_yield = pd.DataFrame([get_yield(i) for i in ticker_list])
    df_duration = pd.DataFrame([get_duration(secid=i, class_code=(df.loc[df['ticker'] == i,
    'class_code']).iloc[0]) for i in ticker_list])

    df_to_sql(df_specs, 'MOEX_bond_specs', 'tinkoffAPIdata.db')
    df_to_sql(df_yield, 'MOEX_bond_yield', 'tinkoffAPIdata.db')
    df_to_sql(df_duration, 'MOEX_bond_duration', 'tinkoffAPIdata.db')

    print('Данные успешно записаны')

report(df)

conn.close()