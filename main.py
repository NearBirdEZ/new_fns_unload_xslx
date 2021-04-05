from thread_version.thread_ver import thread_unload
from asyncio_version.async_ver import async_main
from datetime import datetime as dt
from sys import argv
from library.lib import get_version


def open_request(file):
    """Считываем входные данные заявки"""
    inn_list = []
    rnm_list = []
    count = 0
    with open(file, 'r') as vars_file:
        for line in vars_file:
            if line.strip().startswith('request-number'):
                request_number = line.strip().split('=')[1]
            elif line.strip().startswith('from-Date'):
                start_date = dt.strptime(line.strip().split('=')[1], '%Y-%m-%d').date()
            elif line.strip().startswith('to-Date'):
                end_date = dt.strptime(line.strip().split('=')[1], '%Y-%m-%d').date()
            elif line.strip().startswith('ИНН'):
                count = 1
            elif line.strip().startswith('Регистрационный'):
                count = 2
            elif count == 1 and line.strip() != '':
                inn_list.append(line.strip())
            elif count == 2 and line.strip() != '':
                rnm_list.append(line.strip())
    return request_number, inn_list, rnm_list, start_date, end_date


def return_file(file):
    """По просьбе трудящихся"""
    with open(file, 'r') as f:
        return f.read()


if __name__ == '__main__':
    if not get_version():
        print('Просьба обновить версию скрипта: https://github.com/NearBirdEZ/new_fns_unload_xslx')
        exit(0)

    _, *use_version = argv

    start = dt.now()
    flag_raise = False
    save_file = return_file('request.txt')
    if use_version and use_version[0] == '--asyncio':
        flag_raise = async_main(*open_request('request.txt'))
    elif use_version and use_version[0] == '--threads':
        flag_raise = thread_unload(*open_request('request.txt'))
    else:
        print('Просьба использовать команды:\n\t"python main.py --asyncio"\n\tили\n\t"python main.py --threads"')

    if flag_raise:
        print(f'\n\n{save_file}')
    print('Затраченное время на выгрузку:', dt.now() - start)
