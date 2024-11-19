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


def bonds_by(figi_list: list):
    """
    Получаем информацию с помощью сервиса инструментов bond_by по всем облигациям из моего портфеля
    :param figi_list:
    :return:
    """

    with Client(creds.token_all_only_read) as client:
        r_list = []
        """5.Для каждого FIGI в списке мы:
            5.1. Запрашиваем информацию с помощью сервиса инструментов bond_by
            5.2. Полученную информацию трансформируем в словарь с помощью функции pose_to_dict()
            5.3. Добавляем полученный словарь в список r_list
            5.4. Трансформируем список словарей r_list в датафрейм r_df
            """
        for figi in figi_list:
            r_list.append(pose_to_dict(client.instruments.bond_by(id=figi, id_type=1).instrument))
        r_df = pd.DataFrame(r_list)
        return r_df


def pose_to_dict(p: Bond):
    """
    6.Обрабатываем ответ сервиса инструментов bond_by в удобную для нас форму
    :param p:
    :return:
    """
    r ={
        'figi': p.figi,
        'ticker': p.ticker,
        'uid': p.uid,
        'position_uid': p.position_uid,
        'class_code': p.class_code,
        'isin': p.isin,
        'lot': p.lot,
        'name': p.name,
        'coupon_quantity_per_year': p.coupon_quantity_per_year,
        'maturity_date': p.maturity_date.date(),
        'nominal': cast_money(p.nominal),
        'initial_nominal': cast_money(p.initial_nominal),
        'state_reg_date': p.state_reg_date.date(),
        'placement_date': p.placement_date.date(),
        'placement_price': cast_money(p.placement_price),
        'aci_value': cast_money(p.aci_value),
        'country_of_risk': p.country_of_risk,
        'country_of_risk_name': p.country_of_risk_name,
        'sector': p.sector,
        'issue_size': p.issue_size,
        'issue_size_plan': p.issue_size_plan,
        'trading_status': p.trading_status,
        'floating_coupon_flag': p.floating_coupon_flag,
        'perpetual_flag': p.perpetual_flag,
        'amortization_flag': p.amortization_flag,
        'blocked_tca_flag': p.blocked_tca_flag,
        'subordinated_flag': p.subordinated_flag
    }

    return r


def cast_money(v):
    """
    7.Обрабатываем ответ формата Quatation в удобный формат float
    :param v:
    :return:
    """
    r = v.units + v.nano /1e9
    return r

bonds_by_df = bonds_by(figi_list)

'TCS10A106HB4'
'TCS00A106HB4'
df_to_sql(bonds_by_df, 'API_T_specs', 'tinkoffAPIdata.db')
print('Данные успешно записаны')









