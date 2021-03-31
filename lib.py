import requests
from config import Config
import psycopg2
from aiohttp import ClientSession, BasicAuth
from urllib.request import urlopen
from lxml import etree


def get_version():
    URL = 'https://github.com/NearBirdEZ/new_fns_unload_xslx/blob/master/config.py'
    response = urlopen(URL)
    html_parser = etree.HTMLParser()
    tree = etree.parse(response, html_parser)
    online_version = float(tree.xpath('//*[@id="LC20"]/span[3]/text()')[0])
    return online_version == Config.local_version


class Connections:

    @staticmethod
    def elastic_search(data: str, index: str = '*') -> dict or list:
        """
        На вход принимает запрос для поиска, возвращает json

        Примеры запросов

        {"size" : 1 }

        { "query" : { "bool" : { "must" :
        [{ "term" : {"requestmessage.fiscalDriveNumber.raw" : "9999999999"} },
        {"term" : {"requestmessage.kktRegId.raw" : "7777777777"}},
        {"term" : {"requestmessage.fiscalDocumentNumber" : "888888888"}}] } } }
        """

        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        response = requests.post(f'http://{Config.HOST_EL_PROM}:{Config.PORT_EL_PROM}/{index}/_search',
                                 headers=headers, params=params, data=data,
                                 auth=(Config.USER_EL_PROM, Config.PASSWORD_EL_PROM))
        return response.json()

    @staticmethod
    def elastic_count(data: str, index: str = '*') -> dict or list:
        """
        возвращает значение count
        """

        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        response = requests.post(f'http://{Config.HOST_EL_PROM}:{Config.PORT_EL_PROM}/{index}/_count',
                                 headers=headers, params=params, data=data,
                                 auth=(Config.USER_EL_PROM, Config.PASSWORD_EL_PROM))
        return response.json()['count']

    @staticmethod
    async def async_elastic_search(session: ClientSession, data: str, index: str = '*') -> dict or list:
        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        auth = BasicAuth(login=Config.USER_EL_PROM, password=Config.PASSWORD_EL_PROM, encoding='utf-8')
        async with session.post(f'http://{Config.HOST_EL_PROM}:{Config.PORT_EL_PROM}/{index}/_search',
                                headers=headers, params=params, data=data,
                                auth=auth) as resp:
            return await resp.json()

    @staticmethod
    async def async_elastic_count(session: ClientSession, data: str, index: str = '*') -> dict or list:
        headers = {
            'Content-Type': 'application/json',
        }
        params = (
            ('pretty', ''),
        )

        auth = BasicAuth(login=Config.USER_EL_PROM, password=Config.PASSWORD_EL_PROM, encoding='utf-8')
        async with session.post(f'http://{Config.HOST_EL_PROM}:{Config.PORT_EL_PROM}/{index}/_count',
                                headers=headers, params=params, data=data,
                                auth=auth) as resp:
            response = await resp.json()
            return response.json()['count']

    def __sql(func):
        def wrapper(request: str):
            connect_db = psycopg2.connect(
                database=Config.NAME_DATABASE_PROM,
                user=Config.USER_DB_PROM,
                password=Config.PASSWORD_DB_PROM,
                host=Config.HOST_DB_PROM,
                port=Config.PORT_DB_PROM
            )
            cursor = connect_db.cursor()
            return func(request, connect_db, cursor)

        return wrapper

    @staticmethod
    @__sql
    def sql_select(request: str, *args) -> list:
        """
        На вход подается sql запрос
        На выходе массив построчно.
        :param request:
        :return row: list:
        """
        _, cursor = args
        cursor.execute(request)
        rows = cursor.fetchall()
        return rows

    @staticmethod
    @__sql
    def sql_update(request: str, *args) -> None:
        """
        На вход подается sql запрос
        На выходе массив построчно.
        :param request:
        :return:
        """
        connect_db, cursor = args
        cursor.execute(request)
        connect_db.commit()
        return


if __name__ == '__main__':
    pass
