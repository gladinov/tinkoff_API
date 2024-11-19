from tinkoff.invest import Client, AccessLevel

import creds.creds as creds

"""Функция для получения всех доступных аккаунтов в Tinkoff API"""


def get_accounts():
    accounts = list()
    with Client(creds.token_all_only_read) as client:
        r = client.users.get_accounts()
        for acc in r.accounts:
            if acc.access_level != AccessLevel.ACCOUNT_ACCESS_LEVEL_NO_ACCESS:
                accounts.append(acc.id)

    return accounts

