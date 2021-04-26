import requests
from config import Config
import psycopg2
from aiohttp import ClientSession, BasicAuth
from urllib.request import urlopen
from lxml import etree
import certifi
import sys
import traceback
from datetime import datetime as dt
from datetime import date
import os
import shutil
from typing import List, Tuple
import xlsxwriter
from library.xlsx_const import sys_tax, tagNumber, operationType, width_columns, column_names


def get_version():
    URL = 'https://github.com/NearBirdEZ/new_fns_unload_xslx/blob/master/config.py'
    response = urlopen(URL, cafile=certifi.where())
    html_parser = etree.HTMLParser()
    tree = etree.parse(response, html_parser)
    online_version = float(tree.xpath('//*[@id="LC20"]/span[3]/text()')[0])
    return online_version == Config.local_version


def print_exception() -> None:
    print('Error catch. Traceback lower.')
    exc_type, exc_value, exc_traceback = sys.exc_info()
    traceback.print_exception(exc_type, exc_value, exc_traceback)


class Connections:

    @staticmethod
    def elastic_search(data: str, index: str = '*') -> dict or list:
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


class FnsRequest:
    def __init__(self, request_num: str, inn_list: list, rnm_list: list, start_date: date, end_date: date):
        self.DATE_20_PERCENT_NDS = dt(2019, 1, 1, 0, 0, 0).timestamp()
        self.INDEX = 'receipt.20*,bso,bso_correction,receipt_correction,open_shift,close_shift'
        self.SIZE_UNLOAD_RECEIPT = 5000
        self.request = request_num
        self.inn_list = inn_list
        self.rnm_list = rnm_list
        self.start_date = dt.combine(start_date, dt.min.time()).timestamp()
        self.end_date = dt.combine(end_date, dt.max.time()).timestamp()
        self.raise_flag: bool = False
        self.threads = 3

    """Блок находится в SQLAlchemy. 
    Т.к. тут он не испольуется - функции добавлены в этот класс"""

    def _inn_list_to_string(self):
        return ','.join(f"'{inn}'" for inn in self.inn_list)

    def _rnm_list_to_string(self):
        if self.rnm_list:
            return 'and k.register_number_kkt in (' + ','.join(f"'{rnm}'" for rnm in self.rnm_list) + ')'
        return ''

    def _create_sql_request(self):
        sql_request = f"""
                select c.company_inn, 
                k.register_number_kkt, 
                k.factory_number_kkt, 
                k.human_name, 
                tp.name_traide_point, 
                tp.address_kkt from kkt k 
                inner join company c on c.id = k.company_id
                left join trade_point tp on tp.id = k.trade_point 
                where company_inn in ({self._inn_list_to_string()}) {self._rnm_list_to_string()}"""
        return sql_request

    def get_kkt_information(self) -> list:
        kkt_information_list: list = []
        fields = (
            'company_inn',
            'register_number_kkt',
            'factory_number_kkt',
            'human_name',
            'name_traide_point',  # к сожалению, ошибка в БД
            'address_kkt'
        )
        for rows in Connections().sql_select(self._create_sql_request()):
            tmp_dict = {}
            for row, field in zip(rows, fields):
                tmp_dict.update({field: row if row else ''})
            kkt_information_list.append(tmp_dict)
        return kkt_information_list


def create_work_dir(request) -> None:
    """Создаем рабочую директорию и переходим в нее"""
    if not os.path.exists(f"./unload/{request}/"):
        os.makedirs(f"./unload/{request}/")
    os.chdir(f"./unload/{request}/")


def response_min_max_fd(rnm: str, fn: str, fr: FnsRequest) -> str:
    """Формируем запрос на получение максимального и минимального фискального документа за период"""
    stats_fd_request = """
    {"size" : 0,
    "query" : {
        "bool" : {
            "filter" : {
                "bool" : {
                    "must" : [
                        {"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s" }},
                        {"term" : {"requestmessage.kktRegId.raw" : "%s" }},
                        {"range" : {"requestmessage.dateTime" : {"gte" : "%d", "lte" : "%d" }}}
                            ]
                    }
                }
            }
        }, 
        "aggs" : {
            "stats" : {
                "stats" : {"field" : "requestmessage.fiscalDocumentNumber" }
                    }
                }
            }""" % (fn, rnm, fr.start_date, fr.end_date)
    return stats_fd_request


def response_fn_list(rnm: str, fr: FnsRequest) -> str:
    """Формируем запрос на ФН для РНМ"""
    fn_request = """
    {
        "size": 0,
        "query" : {
            "bool" : {
                "must" : [
                    {"term" : {"requestmessage.kktRegId.raw" : "%s"}},
                    {"range" : {"requestmessage.dateTime" : {"gte" : "%s", "lte" : "%s" }}}
                    ]
                }
            },
        "aggs": {
            "fsIds": {
                "terms": {
                    "field": "requestmessage.fiscalDriveNumber.raw","size": 100
                        }
                    }
                }
    } """ % (rnm, fr.start_date, fr.end_date)
    return fn_request


def check_for_write(parsing_list: List[list],
                    total_sum: float,
                    num_iter: int,
                    iteration: int,
                    count_files: int,
                    kkt_information: dict) -> Tuple[list, int, float]:
    if len(parsing_list) >= 65000 or (num_iter + 1 == iteration and parsing_list):
        count_files += 1
        parsing_list += [[], ['Итоговая сумма ФД за файл, руб.', round(total_sum, 2)]]
        write_xlsx(count_files, parsing_list, kkt_information)
        parsing_list = []
        total_sum = 0
    return parsing_list, count_files, total_sum


def parsing_receipts(receipts: dict, kkt_information: dict, fr: FnsRequest) -> Tuple[List[list], float]:
    parsing_list: list = []
    total_receipts_sum = 0
    for receipt in receipts:
        receipt = receipt['_source']['requestmessage']
        datetime_receipt = receipt.get('dateTime', 0)
        total_sum = (receipt.get('totalSum') or receipt.get('correctionSum', 0)) / 100
        type_operation = receipt.get('operationType')
        base = [receipt.get('user', ''),
                receipt.get('userInn', ''),
                kkt_information['name_traide_point'],
                receipt.get('retailPlaceAddress') or receipt.get('retailAddress', '') or kkt_information['address_kkt'],
                kkt_information['human_name'],  # внутреннее имя ккт
                receipt.get('kktRegId', ''),
                kkt_information['factory_number_kkt'],
                receipt.get('fiscalDriveNumber', ''),
                sys_tax.get(receipt.get('appliedTaxationType') or receipt.get('taxationType'), ''),
                receipt.get('retailPlaceAddress') or receipt.get('retailAddress', ''),
                tagNumber.get(receipt.get('code'), ''),
                receipt.get('shiftNumber', ''),
                receipt.get('requestNumber', ''),
                receipt.get('fiscalDocumentNumber', ''),
                dt.utcfromtimestamp(datetime_receipt).strftime('%Y-%m-%d %H:%M:%S'),
                operationType.get(type_operation, ''),
                total_sum,
                receipt.get('cashTotalSum', 0) / 100,
                receipt.get('ecashTotalSum', 0) / 100,
                receipt.get('nds18', 0) / 100 if datetime_receipt >= fr.DATE_20_PERCENT_NDS and receipt.get(
                    'nds18') else '',  # 20 % ндс
                receipt.get('nds18', 0) / 100 if datetime_receipt < fr.DATE_20_PERCENT_NDS and receipt.get(
                    'nds18') else '',  # 18 % ндс
                receipt.get('nds10', 0) / 100 if receipt.get('nds10') else '',
                receipt.get('nds0', 0) / 100 if receipt.get('nds0') else '',
                receipt.get('ndsNo', 0) / 100 if receipt.get('ndsNo') else '',
                receipt.get('nds18118', 0) / 100 if datetime_receipt >= fr.DATE_20_PERCENT_NDS else '',  # 20/120 % ндс
                receipt.get('nds18118', 0) / 100 if datetime_receipt < fr.DATE_20_PERCENT_NDS else '',  # 18/118 % ндс
                receipt.get('nds10110', 0) / 100 if receipt.get('nds10110') else '',
                receipt.get('prepaidSum', 0) / 100,
                receipt.get('creditSum', 0) / 100,
                receipt.get('provisionSum', 0) / 100,
                receipt.get('buyerPhoneOrAddress', ''),
                receipt.get('buyer', ''),
                receipt.get('buyerInn', ''),
                receipt.get('operator', ''),
                receipt.get('operatorInn', ''),
                receipt.get('fiscalSign', '')]
        if receipt.get('items'):
            items = receipt.get('items')
            if type(receipt.get('items')) == list:
                for item in receipt.get('items'):
                    parsing_list.append(base + get_item_info(item))
            elif type(receipt.get('items')) == dict:
                parsing_list.append(base + get_item_info(items))
            else:
                raise AttributeError('Error in parsing receipts.\n\n', receipt)
        else:
            parsing_list.append(base + ['' for _ in range(7)])

        if type_operation in (2, 3):
            total_receipts_sum -= total_sum
        elif type_operation in (1, 4):
            total_receipts_sum += total_sum
    return parsing_list, round(total_receipts_sum, 2)


def get_item_info(item: dict) -> list:
    lst = [
        item.get('name', ''),
        item.get('unit', ''),
        item.get('productCode', ''),
        item.get('price', 0) / 100,
        round((item.get('unitNds', 0)
               + item.get('nds18118', 0)
               + item.get('nds18', 0)
               + item.get('ndsSum', 0)
               + item.get('nds10', 0)) / 100, 2),
        item.get('quantity', ''),
        item.get('sum', 0) / 100
    ]
    return lst


def response_download_receipt(kkt_information: dict, fr: FnsRequest) -> str:
    """Формируем запрос для получения списка чеков"""
    receipt_request = """
                    {
                        "from" : 0, 
                        "size" : %d, 
                        "_source" : {
                                "includes" : ["requestmessage.*"]
                                    }, 
                           "query" : {
                                "bool" : {
                                    "filter" : {
                                        "bool" : {
                                            "must" : [
                                                {"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s"}}, 
                                                {"term" : {"requestmessage.kktRegId.raw" : "%s"}},
                                                {"range" : {"requestmessage.dateTime" : {"gte" : "%d", "lte" : "%d" }}},
                                                {"range" : {
                                                    "requestmessage.fiscalDocumentNumber" : {"gte" : %d, "lte" : %d }
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }, 
                           "sort" : [
                                { "requestmessage.fiscalDocumentNumber" : { "order" : "asc"}}
                                ]
                        }""" % (fr.SIZE_UNLOAD_RECEIPT,
                                kkt_information['factory_number_fn'],
                                kkt_information['register_number_kkt'],
                                fr.start_date,
                                fr.end_date,
                                kkt_information['min_fd'],
                                kkt_information['max_fd'])
    return receipt_request


def write_xlsx(number_file: int, rows: list, kkt_information: dict) -> None:
    inn = kkt_information['company_inn']
    rnm = kkt_information['register_number_kkt']
    fn = kkt_information['factory_number_fn']
    path = f'./{inn}/{rnm}.{fn}'
    if not os.path.exists(path):
        os.mkdir(path)

    file_name = f'./{path}/{rnm}.{fn}_{number_file}.xlsx'
    wb = xlsxwriter.Workbook(file_name)
    sheet = wb.add_worksheet()

    """set width column"""
    for col, width in width_columns:
        sheet.set_column(f'{col}:{col}', width)

    for i, value in enumerate(column_names + rows):
        for j, val in enumerate(value):
            sheet.write_string(i, j, str(val))
    wb.close()


def zipped(request) -> None:
    shutil.make_archive(f'../{request}', 'zip', '.')
    shutil.rmtree(f'../{request}')


if __name__ == '__main__':
    pass
