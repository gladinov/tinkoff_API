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


df_ticker = df_all[df_all['ticker'] == ticker].sort_values(by='date',ascending=True)
df_ticker = df_ticker[df_ticker.otype.isin([15,22])]
# df_ticker['nkd'] = (-df.payment - df.price * df.quantity)
df_ticker['nkd'] = (abs(df_ticker['payment']) - df_ticker['price'] * df_ticker['quantity'])

print(df_ticker.columns)
print(df_ticker[['date', 'type', 'otype', 'quantity', 'payment', 'price', 'nkd']])

operations = []
for index, row in df_ticker.iterrows():
    print((row['price'], row['quantity']))

    # if row['otype'] == 15:
    #     operations.append((row['price'], row['quantity']))
    # if row['otype'] == 22:
    #     operations.append((row['price'], -row['quantity']))

print(operations)