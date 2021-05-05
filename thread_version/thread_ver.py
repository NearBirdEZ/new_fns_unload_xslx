import os
from threading import Thread, Lock
from library.lib import (
    Connections,
    FnsRequest,
    create_work_dir,
    response_download_receipt,
    parsing_receipts,
    check_for_write,
    response_fn_list,
    response_min_max_fd,
    zipped
)
from library.lib import print_exception
from datetime import date
from math import ceil
import shutil
from typing import Tuple, List
import numpy as np


fr: FnsRequest  # Объявляю глобальную переменную
lock: Lock


def catch_error(func):
    """Wrapper function"""
    def wrapper(*args, **kwargs):
        try:
            if not fr.raise_flag:
                return func(*args, **kwargs)
        except Exception:
            """Ловим ВСЕ ошибки. Нужна стабильность, а не разбор полетов в данном случае"""
            fr.raise_flag = True
            print_exception()
            exit()
    return wrapper


def create_inn_dir(inn: str) -> None:
    lock.acquire()
    if not os.path.exists(f"{os.getcwd()}/{inn}/"):
        os.mkdir(f"{os.getcwd()}/{inn}/")
    lock.release()


@catch_error
def download_receipt(kkt_information: dict) -> None:
    total_parsing_lists = []
    count_files = 0
    total_sum = np.array([0, 0, 0, 0, 0, 0], dtype=np.float64)
    delta: int = kkt_information['max_fd'] - kkt_information['min_fd']
    iteration: int = ceil(delta / fr.SIZE_UNLOAD_RECEIPT)
    for num_iter in range(iteration):
        kkt_information['max_fd'] = (kkt_information['min_fd'] + fr.SIZE_UNLOAD_RECEIPT)
        receipt_request = response_download_receipt(kkt_information, fr)
        receipts = Connections.elastic_search(receipt_request, fr.INDEX)
        parsing_list, receipts_sum = parsing_receipts(receipts['hits']['hits'], kkt_information, fr)

        total_parsing_lists += parsing_list
        total_sum += receipts_sum

        total_parsing_lists, count_files, total_sum = check_for_write(total_parsing_lists, total_sum, num_iter, iteration,
                                                               count_files, kkt_information)
        kkt_information['min_fd'] += (fr.SIZE_UNLOAD_RECEIPT + 1)


@catch_error
def get_min_max_fd(el_request: str) -> Tuple[int or None, int or None]:
    stats = Connections.elastic_search(el_request, fr.INDEX)
    return stats['aggregations']['stats']['min'], stats['aggregations']['stats']['max']


@catch_error
def get_fn_list(el_request: str, rnm: str) -> list:
    response = Connections.elastic_search(el_request, fr.INDEX)
    result = [(rnm, fn['key']) for fn in response['aggregations']['fsIds']['buckets']]
    return result


def run(num_thread: int, kkt_information_list: List[dict]) -> None:
    for i in range(num_thread, len(kkt_information_list), fr.threads):
        kkt_information = kkt_information_list[i]
        create_inn_dir(kkt_information['company_inn'])  # создаю папку для хранения файлов РНМ.ИНН.xlsx
        request_fn = response_fn_list(kkt_information['register_number_kkt'], fr)  # получаю запрос для эластика
        rnm_fn_list = get_fn_list(request_fn, kkt_information['register_number_kkt'])  # получаю список [(рнм, фн), ]
        for rnm, fn in rnm_fn_list:
            request_stats = response_min_max_fd(rnm, fn, fr)  # получаю запрос для эластика
            min_fd, max_fd = get_min_max_fd(request_stats)  # получаю кортеж (min, max)
            if min_fd and max_fd:
                kkt_information.update({'factory_number_fn': fn, 'min_fd': min_fd, 'max_fd': max_fd})
                download_receipt(kkt_information)


def start_threading(kkt_information_list: list) -> None:
    tread_list = []
    for i in range(fr.threads):
        t = Thread(target=run, args=(i, kkt_information_list))
        t.start()
        tread_list.append(t)
    for i in range(fr.threads):
        tread_list[i].join()


def thread_unload(request: str, inn_list: list, rnm_list: list, start_date: date, end_date: date) -> bool:
    global fr, lock
    lock = Lock()
    fr = FnsRequest(request, inn_list, rnm_list, start_date, end_date)
    create_work_dir(request)
    information_kkt_list = fr.get_kkt_information()
    start_threading(information_kkt_list)
    if not fr.raise_flag:
        zipped(request)
        message = f"Выгрузка по заявке № {request} завершена успешно"
    else:
        shutil.rmtree(f'../{request}')
        message = f"Выгрузка по заявке № {request} завершилась с ошибкой. Выгрука удалена."
    print(message)
    return fr.raise_flag


if __name__ == '__main__':
    pass
