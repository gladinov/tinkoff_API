import pandas as pd

import sqlite3

import datetime


def df_to_sql(df: pd.DataFrame, name: str, database: str):
    conn = sqlite3.connect(database)
    df['update_time'] = datetime.datetime.now()
    df.to_sql(name, conn, if_exists='replace', index=False)
    conn.close()
