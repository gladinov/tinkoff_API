"""" Выгрузил данные """

from typing import Optional

from creds import creds as creds
import pandas as pd
from pandas import DataFrame

from tinkoff.invest import Client, RequestError, PortfolioResponse, PositionsResponse, PortfolioPosition, AccessLevel
from tinkoff.invest.services import Services
from pandas.io.excel import ExcelWriter

from save_df_to_sql import df_to_sql
import os

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

"""
Для видео по get_portfolio https://youtu.be/sHu6CxzAmWA
Все видео по API v2 Тинькофф Инвестиции https://www.youtube.com/watch?v=QvPZT5uCU4c&list=PLWVnIRD69wY6j5QvOSU2K_I3NSLnFYiZY

https://tinkoff.github.io/investAPI/operations/#portfoliorequest
https://tinkoff.github.io/investAPI
https://github.com/Tinkoff/invest-python
"""


############################### <=== CLASS ===> ############################



class Hola:
    def __init__(self, client: Services):
        self.usdrur = None
        self.client = client
        self.accounts = []

    def report(self):
        for account_id in self.get_accounts():
            df = self.get_portfolio_df(account_id)
            if df is None: continue
            df_to_sql(df, f'portfolio_acc_{account_id}', 'tinkoffAPIdata.db' )
        print('Данные успешно записаны')


    def get_accounts(self):
        """
        Получаю все аккаунты и буду использовать только те
        кот текущий токен может хотябы читать,
        остальные акк пропускаю
        :return:
        """
        r = self.client.users.get_accounts()
        for acc in r.accounts:
            if acc.access_level != AccessLevel.ACCOUNT_ACCESS_LEVEL_NO_ACCESS:
                self.accounts.append(acc.id)

        return self.accounts

    def get_portfolio_df(self, account_id: str) -> Optional[DataFrame]:
        """
        Преобразую PortfolioResponse в pandas.DataFrame

        :param account_id:
        :return:
        """
        r: PortfolioResponse = self.client.operations.get_portfolio(account_id=account_id)
        if len(r.positions) < 1: return None
        df = pd.DataFrame([self.portfolio_pose_todict(p, account_id) for p in r.positions])
        return df

    def portfolio_pose_todict(self, p: PortfolioPosition, account_id):
        """
        Преобразую PortfolioPosition в dict

        :param p:
        :return:
        """
        r = {
            'account_id': account_id,
            'figi': p.figi,
            'position_uid': p.position_uid,
            'instument_uid': p.instrument_uid,
            'quantity': self.cast_money(p.quantity),
            'expected_yield': self.cast_money(p.expected_yield),
            'instrument_type': p.instrument_type,
            'average_buy_price': self.cast_money(p.average_position_price),
            'current_price': self.cast_money(p.current_price),
            'currency': p.average_position_price.currency,
            'current_nkd': self.cast_money(p.current_nkd),
        }

        return r

    def cast_money(self, v, to_rub=True):
        """
        https://tinkoff.github.io/investAPI/faq_custom_types/
        :param to_rub:
        :param v:
        :return:
        """
        r = v.units + v.nano / 1e9

        return r


def run():
    try:
        with Client(creds.token_all_only_read) as client:
            Hola(client).report()
    except RequestError as e:
        print(str(e))


if __name__ == '__main__':
    run()
