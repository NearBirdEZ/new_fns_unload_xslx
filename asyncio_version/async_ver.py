import asyncio
import os
from aiohttp import ClientSession, ClientTimeout
from library.lib import (
    Connections,
    FnsRequest,
    create_work_dir,
    response_download_receipt,
    parsing_receipts,
    check_for_write,
    response_fn_list,
    response_min_max_fd,
    zipped,
    print_exception
)
from datetime import date
from math import ceil
import shutil
from typing import Tuple, List


def create_inn_dir(inn: str) -> None:
    if not os.path.exists(f"./{inn}/"):
        os.mkdir(f"./{inn}/")


async def download_receipt(session: ClientSession, kkt_information: dict) -> None:
    parsing_list = []
    count_files = 0
    min_fd = kkt_information['min_fd']
    max_fd = kkt_information['max_fd']
    delta: int = max_fd - min_fd
    iteration: int = ceil(delta / fr.SIZE_UNLOAD_RECEIPT)
    for num_iter in range(iteration):
        receipt_request = response_download_receipt(kkt_information, fr)
        receipts = await Connections().async_elastic_search(session, receipt_request, fr.INDEX)
        parsing_list += parsing_receipts(receipts['hits']['hits'], kkt_information, fr)
        parsing_list, count_files = check_for_write(parsing_list, num_iter, iteration, count_files, kkt_information)
        min_fd += fr.SIZE_UNLOAD_RECEIPT


async def get_min_max_fd(session: ClientSession, el_request: str) -> Tuple[int or None, int or None]:
    stats = await Connections().async_elastic_search(session, el_request, fr.INDEX)
    return stats['aggregations']['stats']['min'], stats['aggregations']['stats']['max']


async def get_fn_list(session: ClientSession, el_request: str, rnm: str) -> list:
    response = await Connections().async_elastic_search(session, el_request, fr.INDEX)
    return [(rnm, fn['key']) for fn in response['aggregations']['fsIds']['buckets']]


async def do_one_rnm(session: ClientSession, kkt_information: dict) -> None:
    create_inn_dir(kkt_information['company_inn'])  # создаю папку для хранения файлов РНМ.ИНН.xlsx
    request_fn = response_fn_list(kkt_information['register_number_kkt'], fr)  # получаю список [(рнм, фн), ]
    rnm_fn_list = await get_fn_list(session, request_fn, kkt_information['register_number_kkt'])
    for rnm, fn in rnm_fn_list:
        request_stats = response_min_max_fd(rnm, fn, fr)
        min_fd, max_fd = await get_min_max_fd(session, request_stats)
        if min_fd and max_fd:
            kkt_information.update({'factory_number_fn': fn, 'min_fd': min_fd, 'max_fd': max_fd})
            await download_receipt(session, kkt_information)


async def run(inn_rnm_list: List[dict]) -> None:
    tasks = []
    async with ClientSession(timeout=ClientTimeout(total=10**10)) as session:
        for row in inn_rnm_list:
            task = asyncio.ensure_future(do_one_rnm(session, row))
            tasks.append(task)
        await asyncio.gather(*tasks)


fr: FnsRequest  # Объявляю глобальную переменную


def async_main(request: str, inn_list: list, rnm_list: list, start_date: date, end_date: date) -> bool:
    global fr
    flag_raise = False
    fr = FnsRequest(request, inn_list, rnm_list, start_date, end_date)
    create_work_dir(request)
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(fr.get_kkt_information()))
    try:
        loop.run_until_complete(future)
        zipped(request)
        message = f"Выгрузка по заявке № {request} завершена успешно"
    except Exception:
        print_exception()
        shutil.rmtree(f'../{request}')
        message = f"Выгрузка по заявке № {request} завершилась с ошибкой. Выгрука удалена."
        flag_raise = True
    print(message)
    return flag_raise


if __name__ == '__main__':
    pass
