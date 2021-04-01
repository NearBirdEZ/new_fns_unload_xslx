import asyncio
import os
from aiohttp import ClientSession
from lib import Connections, print_exception
from datetime import datetime as dt
from datetime import date, timedelta
from math import ceil
from asyncio_version.xlsx_const import sys_tax, tagNumber, operationType, width_columns, column_names
import xlsxwriter
import random
import string
import shutil
from typing import Tuple, List


def create_work_dir() -> None:
    """Создаем рабочую директорию и переходим в нее"""
    if not os.path.exists(f"./unload/{fr.request}/"):
        os.makedirs(f"./unload/{fr.request}/")
    os.chdir(f"./unload/{fr.request}/")


def create_inn_dir(inn: str) -> None:
    if not os.path.exists(f"./{inn}/"):
        os.mkdir(f"./{inn}/")


def response_min_max_fd(rnm: str, fn: str) -> str:
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


def response_fn_list(rnm: str) -> str:
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
                    "field": "requestmessage.fiscalDriveNumber.raw","size": 500000
                        }
                    }
                }
    } """ % (rnm, fr.start_date, fr.end_date)
    return fn_request


def check_for_write(total_parsing_list: List[list], num_iter: int, iteration: int, count_files: int, inn: str, rnm: str,
                    fn: str) -> Tuple[list, int]:
    if len(total_parsing_list) >= 65000 or (num_iter + 1 == iteration and total_parsing_list):
        count_files += 1
        write_xlsx(count_files, inn, rnm, fn, total_parsing_list)
        total_parsing_list = []
    return total_parsing_list, count_files


def parsing_receipts(receipts: dict, kkt_information: dict) -> List[list]:
    parsing_list: list = []
    for receipt in receipts:
        receipt = receipt['_source']['requestmessage']
        datetime_receipt = receipt.get('dateTime', 0) + 10800

        base = [receipt.get('user', ''),
                receipt.get('userInn', ''),
                kkt_information['name_traide_point'],
                kkt_information['address_kkt'] or receipt.get('retailPlaceAddress') or receipt.get('retailAddress', ''),
                kkt_information['human_name'],  # внутреннее имя ккт
                receipt.get('kktRegId', ''),
                kkt_information['factory_number_kkt'],
                receipt.get('fiscalDriveNumber', ''),
                sys_tax.get(receipt.get('appliedTaxationType'), ''),
                receipt.get('retailPlaceAddress') or receipt.get('retailAddress', ''),
                tagNumber.get(receipt.get('code'), ''),
                receipt.get('shiftNumber', ''),
                receipt.get('requestNumber', ''),
                receipt.get('fiscalDocumentNumber', ''),
                dt.utcfromtimestamp(datetime_receipt).strftime('%Y-%m-%d %H:%M:%S'),
                operationType.get(receipt.get('operationType'), ''),
                receipt.get('totalSum', 0) / 100,
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
            for item in receipt.get('items'):
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
                parsing_list.append(base + lst)
        else:
            parsing_list.append(base + ['' for _ in range(7)])
    return parsing_list


def response_download_receipt(rnm: str, fn: str, min_fd: int, max_fd: int) -> str:
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
                        }""" % (fr.SIZE_UNLOAD_RECEIPT, fn, rnm, fr.start_date, fr.end_date, min_fd, max_fd)
    return receipt_request


def write_xlsx(number_file: int, inn: str, rnm: str, fn: str, rows: list) -> None:
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


def zipped() -> None:
    shutil.make_archive(f'../{fr.request}', 'zip', '.')
    shutil.rmtree(f'../{fr.request}')


async def download_receipt(session: ClientSession, rnm: str, fn: str, min_fd: int, max_fd: int,
                           kkt_information: dict) -> None:
    total_parsing_list = []
    count_files = 0
    delta: int = max_fd - min_fd
    iteration: int = ceil(delta / fr.SIZE_UNLOAD_RECEIPT)
    for num_iter in range(iteration):
        receipt_request = response_download_receipt(rnm, fn, min_fd, max_fd)
        receipts = await fr.CONNECT.async_elastic_search(session, receipt_request, fr.INDEX)
        total_parsing_list += parsing_receipts(receipts['hits']['hits'], kkt_information)
        inn = kkt_information['company_inn']
        total_parsing_list, count_files = check_for_write(total_parsing_list, num_iter, iteration, count_files, inn,
                                                          rnm, fn)
        min_fd += fr.SIZE_UNLOAD_RECEIPT


async def get_min_max_fd(session: ClientSession, el_request: str) -> Tuple[int or None, int or None]:
    stats = await fr.CONNECT.async_elastic_search(session, el_request, fr.INDEX)
    return stats['aggregations']['stats']['min'], stats['aggregations']['stats']['max']


async def get_fn_list(session: ClientSession, el_request: str, rnm: str):
    response = await fr.CONNECT.async_elastic_search(session, el_request, fr.INDEX)
    return ((rnm, fn['key']) for fn in response['aggregations']['fsIds']['buckets'])


async def do_one_rnm(session: ClientSession, kkt_information: dict) -> None:
    create_inn_dir(kkt_information['company_inn'])  # создаю папку для хранения файлов РНМ.ИНН.xlsx
    request_fn = response_fn_list(kkt_information['register_number_kkt'])  # получаю список [(рнм, фн), ]
    rnm_fn_list = await get_fn_list(session, request_fn, kkt_information['register_number_kkt'])
    for rnm, fn in rnm_fn_list:
        request_stats = response_min_max_fd(rnm, fn)
        min_fd, max_fd = await get_min_max_fd(session, request_stats)
        if min_fd and max_fd:
            await download_receipt(session, rnm, fn, min_fd, max_fd, kkt_information)


async def run(inn_rnm_list: List[dict]) -> None:
    tasks = []
    async with ClientSession() as session:
        for row in inn_rnm_list:
            task = asyncio.ensure_future(do_one_rnm(session, row))
            tasks.append(task)
        await asyncio.gather(*tasks)


class FnsRequest:
    def __init__(self, request_num: str, inn_list: list, rnm_list: list, start_date: date, end_date: date):
        self.DATE_20_PERCENT_NDS = dt(2019, 1, 1, 0, 0, 0).timestamp()
        self.INDEX = 'receipt.20*,bso*,*_shift'
        self.SIZE_UNLOAD_RECEIPT = 5000
        self.CONNECT = Connections()
        self.request = request_num
        self.unique_request = self.build_block()
        self.inn_list = inn_list
        self.rnm_list = rnm_list
        self.start_date = dt.combine(start_date, dt.min.time()).timestamp()
        self.end_date = (dt.combine(end_date, dt.min.time()) + timedelta(hours=23, minutes=59, seconds=59)).timestamp()

    def build_block(self) -> str:
        """Формирование уникальногоо идентификатора для названия файла"""
        return f"{self.request}_{''.join(random.choice(string.ascii_letters) for _ in range(10))}"

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
        for rows in self.CONNECT.sql_select(self._create_sql_request()):
            tmp_dict = {}
            for row, field in zip(rows, fields):
                tmp_dict.update({field: row if row else ''})
            kkt_information_list.append(tmp_dict)
        return kkt_information_list


fr: FnsRequest  # Объявляю глобальную переменную


def async_main(request: str, inn_list: list, rnm_list: list, start_date: date, end_date: date) -> bool:
    global fr
    flag_raise = False
    fr = FnsRequest(request, inn_list, rnm_list, start_date, end_date)
    create_work_dir()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(fr.get_kkt_information()))
    try:
        print('Выгрузка запущена')
        loop.run_until_complete(future)
        zipped()
        message = f'Выгрузка {fr.request} успешно выполнена'
    except Exception:
        print_exception()
        shutil.rmtree(f'../{fr.request}')
        message = f'Выгрузка {fr.request} завершилась с ошибкой'
        flag_raise = True
    print(message)
    return  flag_raise

if __name__ == '__main__':
    pass
