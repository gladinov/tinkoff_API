import sqlite3
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


conn = sqlite3.connect('tinkoffAPIdata.db')
df = pd.read_sql_query("SELECT * FROM API_T_specs", conn)
# for i in df['ticker'].values:
ticker = "RU000A1077V4"


df_op_1 = pd.read_sql_query("SELECT * FROM bond_operations_acc_2007907898", conn)
df_op_2 = pd.read_sql_query("SELECT * FROM bond_operations_acc_2016119489", conn)
df_all = pd.concat([df_op_1, df_op_2], ignore_index=True).sort_values(by='date',
                                                             ascending=False)

print(df_all.columns)
df_ticker = df_all[df_all['ticker'] == ticker]
print(df_ticker[['ticker','date','type','otype','quantity']])

# for index, row in df_ticker.iterrows():
#     if