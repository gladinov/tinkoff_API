from creds import creds as creds
import pandas as pd

import sqlite3


from tinkoff.invest import (BondResponse,FindInstrumentResponse,InstrumentShort, Bond,InstrumentIdType,
                            Client, RequestError, PortfolioResponse, PositionsResponse, PortfolioPosition, AccessLevel)
from tinkoff.invest.services import Services

from accounts import get_accounts

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


"""Пока выведем данную функкию в отдельный файл. Потом можно его добавить в операции.
Данная файл обрабатывает операции с облигациями и создает отдельные файлы покаждому аккаунту 
со списком (isin, ticker, class_code, uid, position_uid) для каждой облигации.
В дальнейшем данную информацию можно будет """



conn = sqlite3.connect('tinkoffAPIdata.db')
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type ='table';")
table_names = [i[0] for i in list(cursor)]
table_name = f'acc_2007907898_operations'
df_table = pd.read_sql_query(f"SELECT * FROM {table_name} where instrument_type == 'bond'", conn)
uids = df_table['instrument_uid'].unique()



def run():
    try:
        with Client(creds.token_all_only_read) as client:
            Hola(client).report(uids)
    except RequestError as e:
        print(str(e))

class Hola:
    def __init__(self, client:Services):
        self.client = client
        self.accounts = get_accounts()


    def report(self):
        conn = sqlite3.connect('tinkoffAPIdata.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type ='table';")
        table_names = [i[0] for i in list(cursor)]

        for account_id in self.accounts:
            operations_table = f'acc_{account_id}_operations'
            """ЗДЕСЬ ОСТАНОВИЛСЯ. НУЖНО ПРОВЕРИТЬ НАЛИЧИЕ ТАБЛИЦЫ ТИТЛОВ И ПРОПИСАТЬ ДЕЙСТВИЯ ПРИ ЕЕ ОТСУТВИИ"""
            # titles_table =
            if table_name in table_names:
                pass
            else:
                df_table = pd.read_sql_query(f"SELECT * FROM {table_name} where instrument_type == 'bond'", conn)
                uids = df_table['instrument_uid'].unique()
                self.get_titles(uids).to_sql(f'acc_{account_id}_titles',conn,  )



    def get_titles(self, uids):
        result_uids = list()
        with Client(creds.token_all_only_read) as client:
            for uid in uids:
                r = client.instruments.find_instrument(query=uid)
                try:
                    result_uids.append(self.uids_todict(r))
                except RequestError as e:
                    print(str(e))

        return pd.DataFrame(result_uids)

    def uids_todict(self, u:FindInstrumentResponse):
        r = {
            'isin': u.instruments[0].isin,
            'ticker': u.instruments[0].ticker,
            'class_code': u.instruments[0].class_code,
            'instrument_type': u.instruments[0].instrument_type,
            'name': u.instruments[0].name,
            'uid': u.instruments[0].uid,
            'position_uid': u.instruments[0].position_uid
        }
        return r


run()

conn.close()
