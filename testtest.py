from typing import Optional

from creds import creds as creds


from tinkoff.invest import (Client, RequestError, PortfolioResponse, PositionsResponse,
                            PortfolioPosition, AccessLevel, BondResponse, Bond)
from tinkoff.invest.services import Services

import sqlite3

from save_df_to_sql import df_to_sql

import pandas as pd


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


"""1.Открываем файл,который получаем в программе portfolio_v1.py """
conn = sqlite3.connect("tinkoffAPIdata.db")
df = pd.read_sql_query("SELECT * FROM all_portfolio", conn)


def bond_rub(_df: pd.DataFrame):
    """
    2.Фильтруем данные датафрейма и оставляем только рублевые облигации
    """
    bond_rub_df = _df[(_df['instrument_type'] =='bond') & (_df['currency'] == 'rub')]

    return bond_rub_df

"""3.Получаем результат функции bond_rub() и сохраняем его в датафрейм"""
bonds_rub_df = bond_rub(df)
"""4.Получаеим список всех FIGI облигаций в портфеле"""
figi_list = list(set(bonds_rub_df.figi))
print(figi_list)

