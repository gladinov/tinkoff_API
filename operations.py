import os
from datetime import datetime, timedelta
from typing import Optional

from creds import creds
import pandas as pd
from pandas import DataFrame, ExcelWriter

import sqlite3

from tinkoff.invest import Client, RequestError, PositionsResponse, AccessLevel, OperationsResponse, Operation, \
    OperationState, OperationType
from tinkoff.invest.services import Services

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def run():
    try:
        with Client(creds.token_all_only_read) as client:
            Hola(client).report()
    except RequestError as e:
        print(str(e))


def df_to_sql(conn, df: pd.DataFrame, account_id):
    # df['update_time'] = datetime.now()
    df.to_sql(f'acc_{account_id}_operations', conn, if_exists='replace', index=False)


class Hola:
    def __init__(self, client: Services):
        self.client = client
        self.accounts = []

    def report(self):

        """Проверяем базу данных на наличие таблицы"""
        conn = sqlite3.connect('tinkoffAPIdata.db')
        cursor = conn.cursor()

        """Получаем список названий всех таблиц в базе данных tinkoffAPIdata.db"""
        cursor.execute("SELECT name FROM sqlite_master WHERE type ='table';")
        tables_names = [i[0] for i in list(cursor)]

        for account_id in self.get_accounts():
            table_name = f'acc_{account_id}_operations'
            """Проверяем по условию существует ли данная таблица в БД"""
            if table_name in tables_names:
                """Если да, то выгружаем таблицу в датафрейм
                 и получаем дату последней записанной операции"""
                df_table = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

                """ С timedelta Костыль, но пока нет идей как сделать красиво"""
                if df_table.empty:
                    from_ = datetime(2015, month=1, day=1)
                else:
                    from_ = (datetime.fromisoformat(df_table.sort_values(by='date', ascending=False).iloc[0]['date'])
                            + timedelta(microseconds=1))

                """Запрашиваем данные с Тинькофф АПИ с момента последней операции"""
                df = self.get_operations_df(account_id,
                                            from_,
                                            datetime.utcnow())
                if df is None: continue
                """Объединяем датафрейм из таблицы и полученный новый датафрейм"""
                r = df.drop_duplicates()

                """Сохраняем ДФ в БД"""
                r.to_sql(f'acc_{account_id}_operations', conn, if_exists='append', index=False)
                print(f'В таблицу "acc_{account_id}_operations" добавлено {r.shape[0]} строк')
            else:
                """Если талбицы нет в БД, то значит данные не загружались. Выгружаем все данные"""
                df = self.get_operations_df(account_id,
                                            datetime(2015, month=1, day=1),
                                            datetime.utcnow())
                if df is None: continue

                df_to_sql(conn, df, account_id)
                print(f'В таблицу "acc_{account_id}_operations" добавлено {df.shape[0]} строк')
        conn.close()

    def get_accounts(self):
        r = self.client.users.get_accounts()
        for acc in r.accounts:
            if acc.access_level != AccessLevel.ACCOUNT_ACCESS_LEVEL_NO_ACCESS:
                self.accounts.append(acc.id)

        return self.accounts

    def get_operations_df(self, account_id: str, from_, to) -> Optional[DataFrame]:
        dfs = []
        while True:
            r: OperationsResponse = self.client.operations.get_operations(
                account_id=account_id,
                from_=from_,
                to=to
            )

            if len(r.operations) < 1:
                return pd.concat(dfs, ignore_index=True).drop_duplicates() if len(dfs) > 0 else None

            df = pd.DataFrame([self.operation_todict(p, account_id) for p in r.operations])
            to = (df.sort_values(by='date', ascending=False).iloc[-1]['date'].to_pydatetime()
                     - timedelta(microseconds=1))
            dfs.append(df)

    def operation_todict(self, o: Operation, account_id: str):
        """
        Преобразую PortfolioPosition в dict
        :param p:
        :return:
        """
        r = {
            'acc': account_id,
            'id': o.id,
            'figi': o.figi,
            'instrument_uid': o.instrument_uid,
            'position_uid': o.position_uid,
            'parent_id': o.parent_operation_id,
            'date': o.date,
            'type': o.type,
            'otype': o.operation_type,
            'currency': o.currency,
            'instrument_type': o.instrument_type,
            'quantity': o.quantity,
            'state': o.state,
            'payment': self.cast_money(o.payment),
            'price': self.cast_money(o.price),
            'update_time' : datetime.now()
        }

        return r

    def cast_money(self, v):
        """
        https://tinkoff.github.io/investAPI/faq_custom_types/
        :param v:
        :return:
        """
        r = v.units + v.nano / 1e9

        return r


run()
