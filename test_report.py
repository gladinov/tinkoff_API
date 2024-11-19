import sqlite3
import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


conn = sqlite3.connect('tinkoffAPIdata.db')
df = pd.read_sql_query("SELECT * FROM API_T_specs", conn)
# for i in df['isin'].values:
#     print(i)
# isin = "RU000A106HB4"


df_op_1 = pd.read_sql_query("SELECT * FROM isin_acc_2007907898_operations", conn)
# df_op_2 = pd.read_sql_query("SELECT * FROM bond_operations_acc_2016119489", conn)
# df_all = pd.concat([df_op_1, df_op_2], ignore_index=True).sort_values(by='date',
#                                                              ascending=False)



def get_average_buy_price_from_operations(df: pd.DataFrame, isin: str):
    df_isin = df[df['isin'] == isin].sort_values(by='date', ascending=True)
    df_isin['quantity_end'] = df_isin['quantity']
    buy_operations =[[row['otype'],row['price'], row['quantity'], row['quantity_end']]
                     for index, row in df_isin.iterrows() if row['otype'] == 15]
    # sell_operations =[[row['otype'],
    #                    row['price'],
    #                    row['quantity'],
    #                    row['quantity_end']]
    #                   for index, row in df_isin.iterrows() if row['otype'] == 22]

    for index, row in df_isin.iterrows():
        sell_operations = []
        if row['otype'] == 22:
            sell_operations.append([row['otype'], row['price'],row['quantity'], row['quantity_end']])




    print(isin, buy_operations)
    print(isin, sell_operations)

    for sell_op in sell_operations:
        for buy_op in buy_operations:
            if buy_op[3] >= sell_op[3]:
                buy_op[3] = buy_op[3] - sell_op[3]
                sell_op[3] = 0
                break
            else:
                sell_op[3] = sell_op[3] - buy_op[3]
                buy_op[3] = 0
                continue


    average_price = []
    bond_count = 0
    for i in buy_operations:
        average_price.append(i[1] * i[3])
        bond_count += i[3]

    return sum(average_price)/bond_count




for isin in df['isin'].values:
    print('\n')
    try:
        a = get_average_buy_price_from_operations(df_op_1, isin)
        print(round(a, 2))
    except Exception as e:
        print(f'Произошла ошибка: {e}')

# try:
#     a = get_average_buy_price_from_operations(df_op_1, 'RU000A106HB4')
#     print(a)
# except Exception as e:
#     print(f'Произошла ошибка: {e}')









