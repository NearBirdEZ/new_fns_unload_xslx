#!/usr/bin/env python3

from lib import Connections
import datetime as dt
import os
from math import ceil
import zipfile
from threading import Thread, Lock
import sys
import xlsxwriter


class UnloadFns:

    def __init__(self, inner_vars_file):
        inner_vars = self.__open_request(inner_vars_file)
        self.request = inner_vars[0]
        self.threads = 3
        self.date_list = self.__division_by_month(inner_vars[1])
        self.inn_string = inner_vars[2]
        self.rnm_string = inner_vars[3]
        self.connect = Connections()
        self.__job_folder()
        self.lock = Lock()
        self.STOP_FLAG = False
        self.exception = None

    def __open_request(self, file):
        """Считываем входные данные заявки"""
        inn_list = []
        rnm_list = []
        date_in = []
        count = 0
        with open(file, 'r') as vars:
            for line in vars:
                if line.strip().startswith('request-number'):
                    request_number = line.strip().split('=')[1]
                elif line.strip().startswith('from-Date'):
                    date_in.append(line.strip().split('=')[1])
                elif line.strip().startswith('to-Date'):
                    date_in.append(line.strip().split('=')[1])
                elif line.strip().startswith('ИНН'):
                    count = 1
                elif line.strip().startswith('Регистрационный'):
                    count = 2
                elif count == 1 and line.strip() != '':
                    inn_list.append(line.strip())
                elif count == 2 and line.strip() != '':
                    rnm_list.append(line.strip())
            inn_string = ', '.join(f"'{inn}'" for inn in inn_list)
            if len(rnm_list) != 0:
                rnm_string = ', '.join(f"'{rnm}'" for rnm in rnm_list)
                rnm_string = f'and k.register_number_kkt in ({rnm_string})'
            else:
                rnm_string = ''
        return request_number, date_in, inn_string, rnm_string

    def __division_by_month(self, date_in):
        # переводим дату в timestamp
        time1 = dt.datetime.fromisoformat(date_in[0])
        time2 = dt.datetime.fromisoformat(date_in[1]) + dt.timedelta(hours=23, minutes=59, seconds=59)
        return int(time1.timestamp()), int(time2.timestamp())

    def __job_folder(self):
        """Создаем рабочую директорию и переходим в нее"""
        if not os.path.exists(f"./unload/"):
            os.mkdir(f"./unload/")

        if not os.path.exists(f"./unload/{self.request}/"):
            os.mkdir(f"./unload/{self.request}/")
        os.chdir(f"./unload/{self.request}/")

    def get_job_dict(self) -> dict:
        inn_rnm_fn_dict = {}
        sql_req = f"""
            select c.company_inn, k.factory_number_kkt, k.register_number_kkt, fs_id from stats.by_kkt bk 
            inner join kkt k on cast(k.register_number_kkt as bigint) = bk.kkt_reg_id
            inner join company c on c.id = k.company_id 
            where c.company_inn in ({self.inn_string}) {self.rnm_string}
            and  first_date_time < '{self.date_list[1]}'
            and  last_date_time > '{self.date_list[0]}'"""

        for inn, factory_number_kkt, rnm, fn in self.connect.sql_select(sql_req):
            if inn_rnm_fn_dict.get(inn):
                inn_rnm_fn_dict[inn] += [(factory_number_kkt, rnm, fn)]
            else:
                inn_rnm_fn_dict[inn] = [(factory_number_kkt, rnm, fn)]
        return inn_rnm_fn_dict

    def min_max_fd(self, rnm: str, fn: str, start_date: int, end_date: int) -> tuple:
        """Получаем минимальный и максимальные ФД в периоде относительно РНМ и ФН"""
        stats_fd_request = '{"query" : {"bool" : {"filter" : {"bool" : {"must" : ' \
                           '[{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s" }},' \
                           '{"term" : {"requestmessage.kktRegId.raw" : "%s" }}, ' \
                           '{"range" : {"requestmessage.dateTime" : {"gte" : "%d", "lte" : "%d" }}}]}}}}, ' \
                           '"aggs" : {"stats" : { "stats" : { "field" : "requestmessage.fiscalDocumentNumber" }}}}' % (
                               fn, rnm, start_date, end_date)
        stats = self.connect.to_elastic(stats_fd_request, 'receipt*,bso*,*_shift')['aggregations']['stats']
        return stats['min'], stats['max']

    @staticmethod
    def get_information_on_receipt(receipt: dict, num_kkt: str) -> list:

        receipt = receipt['_source']['requestmessage']
        sys_tax = {1: "ОСН",
                   2: "УСН доход",
                   4: "УСН доход-расход",
                   8: "ЕНВД",
                   16: "ЕСХН",
                   32: "Патент"}

        tagNumber = {1: "Отчет о регистрации",
                     2: "Отчет об открытии смены",
                     3: "Кассовый чек",
                     4: "БСО",
                     5: "Отчёт о закрытии смены",
                     6: "Отчёт о закрытии фискального накопителя",
                     11: "Отчёт об изменении параметров регистрации",
                     21: "Отчёт о текущем состоянии расчетов",
                     31: "Кассовый чек коррекции",
                     41: "Бланк строгой отчетности коррекции"}

        operationType = {1: "Приход",
                         2: "Возврат прихода",
                         3: "Расход",
                         4: "Возврат расхода"}

        date_to_20 = dt.datetime(year=2019, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        datetime_rec = int(receipt.get('dateTime', 0)) + 10800
        date_eq = dt.datetime.utcfromtimestamp(datetime_rec)

        nds = {1: "НДС 20%" if date_eq >= date_to_20 else "НДС 18%",
               2: "НДС 10%",
               3: "НДС 20/120",
               4: "НДС 10/110",
               5: "НДС 0%",
               6: "НДС не облагается"}

        datetime_rec = dt.datetime.utcfromtimestamp(datetime_rec).strftime('%Y-%m-%d %H:%M:%S')  # дата получения чека

        nds18 = int(receipt.get('nds18', 0)) / 100 if date_eq < date_to_20 and receipt.get('nds18') else ''
        nds20 = int(receipt.get('nds18', 0)) / 100 if date_eq >= date_to_20 and receipt.get('nds18') else ''
        nds10 = int(receipt.get('nds10', 0)) / 100 if receipt.get('nds10') else ''
        nds0 = int(receipt.get('nds0', 0)) / 100 if receipt.get('nds0') else ''
        nds18118 = int(receipt.get('nds18118', 0)) / 100 if date_eq < date_to_20 else ''
        nds20120 = int(receipt.get('nds18118', 0)) / 100 if date_eq >= date_to_20 else ''
        nds10110 = int(receipt.get('nds10110', 0)) / 100 if receipt.get('nds10110') else ''
        ndsno = int(receipt.get('ndsNo', 0)) / 100 if receipt.get('ndsNo') else ''

        rec_list = []
        base = [receipt.get('user', ''),
                receipt.get('userInn', ''),
                receipt.get('retailPlace', ''),
                receipt.get('retailPlaceAddress') or receipt.get('retailAddress', ''),
                '',  # внутреннее имя
                receipt.get('kktRegId', ''),
                num_kkt,
                receipt.get('fiscalDriveNumber', ''),
                sys_tax.get(receipt.get('appliedTaxationType'), ''),
                receipt.get('retailPlaceAddress') or receipt.get('retailAddress', ''),
                tagNumber.get(receipt.get('code'), ''),
                receipt.get('shiftNumber', ''),
                receipt.get('requestNumber', ''),
                receipt.get('fiscalDocumentNumber', ''),
                datetime_rec,
                operationType.get(receipt.get('operationType'), ''),
                receipt.get('totalSum', 0) / 100,
                receipt.get('cashTotalSum', 0) / 100,
                receipt.get('ecashTotalSum', 0) / 100,
                nds20,
                nds18,
                nds10,
                nds0,
                ndsno,
                nds20120,
                nds18118,
                nds10110,
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
                    int(item.get('price', 0)) / 100,
                    int(item.get('unitNds', 0)) / 100,
                    # nds.get(item.get('nds'), ''),
                    item.get('quantity', ''),
                    int(item.get('sum', 0)) / 100
                ]
                rec_list.append(base + lst)
        else:
            rec_list.append(base + ['' for _ in range(7)])
        return rec_list

    def download_json(self, inn: str, num_kkt: str, rnm: str, fn: str, min_fd: int, max_fd: int, num: int) -> bool:
        """Основной скрипт выгрузки
        Формируется запрос согласно максимального и минимального ФД по РНМ:ФН
        Выгружаются по всем необходимым индексам
        Флаг необходим для запуска функции архивирования"""
        flag = False

        index_list = ['receipt.*', 'bso', 'bso_correction', 'close_shift', 'open_shift']
        count = 0
        delta = max_fd - min_fd
        iteration = ceil(delta / 5000)
        rec_list = []
        for i, type_fd in enumerate(index_list):
            for j in range(iteration):
                data = """
                    {
                        "from" : 0, 
                        "size" : 5000, 
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
                        }""" % (fn, rnm, self.date_list[0], self.date_list[1], min_fd, max_fd)

                receipts = self.connect.to_elastic(data, type_fd)['hits']['hits']
                for receipt in receipts:
                    """формируем список товаров"""
                    rec_list += (self.get_information_on_receipt(receipt, num_kkt))
                """если количество товаров больше или равно 65к или это оставшиеся товары, то записать в xsls файл"""
                if len(rec_list) >= 65000 or (i == 4 and j + 1 == iteration and rec_list):
                    flag = True
                    count += 1
                    self.write_xlsx(count, inn, rnm, fn, rec_list, num)
                    rec_list = []
                min_fd += 5000
            """Возвращаем минимальное значение ФД"""
            min_fd = max_fd - delta
        return flag

    @staticmethod
    def write_xlsx(number_file: int, inn: str, rnm: str, fn: str, rows: list, num: int) -> None:
        width_col = (("A", 52), ("B", 14), ("C", 27), ("D", 42), ("E", 22), ("F", 27), ("G", 21), ("H", 20),
                     ("I", 37), ("J", 42), ("K", 27), ("L", 13), ("M", 19), ("N", 21), ("O", 21), ("P", 17),
                     ("Q", 16), ("R", 22), ("S", 26), ("T", 21), ("U", 21), ("V", 21), ("W", 22), ("X", 21),
                     ("Y", 24), ("Z", 24), ("AA", 24), ("AB", 27), ("AC", 30), ("AD", 33), ("AE", 19),
                     ("AF", 25), ("AG", 26), ("AH", 17), ("AI", 14), ("AJ", 20), ("AK", 100), ("AL", 36),
                     ("AM", 27), ("AN", 60), ("AO", 40), ("AP", 29), ("AQ", 33),)
        column_names = [(
            'Наименование налогоплательщика',
            'ИНН',
            'Название торговой точки',
            'Адрес торговой точки',
            'Внутреннее имя ККТ',
            'Регистрационный номер ККТ',
            'Заводской номер ККТ',
            'Заводской номер ФН',
            'Применяемая система налогообложения',
            'Адрес расчетов',
            'Тип фискального документа',
            'Номер смены',
            'Номер ФД за смену',
            'Порядковый номер ФД',
            'Дата и время ФД',
            'Признак расчета',
            'Сумма ФД, руб.',
            'Сумма наличные, руб.',
            'Сумма электронно, руб.  ',
            'Сумма НДС 20%, руб.',
            'Сумма НДС 18%, руб.',
            'Сумма НДС 10%, руб.',
            'Сумма c НДС 0%, руб.',
            'Сумма без НДС, руб.',
            'Сумма НДС 20/120, руб.',
            'Сумма НДС 18/118, руб.',
            'Сумма НДС 10/110, руб.',
            'Сумма предоплатой (аванс)',
            'Сумма постоплатой (в кредит)',
            'Сумма встречным предоставлением',
            'Абонентский адрес',
            'Покупатель (клиент)',
            'ИНН покупателя (клиента)',
            'Кассир',
            'ИНН кассира',
            'Фискальный признак',
            'Наименование предмета расчета',
            'Единица измерения предмета расчета',
            'Код товара',
            'Цена за единицу предмета расчета с учетом скидок и наценок',
            'Размер НДС за единицу предмета расчета',
            'Количество предмета расчета',
            'Итоговая сумма предмета расчета'
        )]
        try:
            """В связи с тем, что несколько потоков пытаются создать папку, if не успевает. lock не вижу смысла"""
            if not os.path.exists(f"./{inn}/"):
                os.mkdir(f"./{inn}/")
        except FileExistsError:
            pass

        file_name = f'./{inn}/{rnm}.{fn}_{number_file}@{num}.xlsx'

        wb = xlsxwriter.Workbook(file_name)
        sheet = wb.add_worksheet()

        """set width column"""
        for col, width in width_col:
            sheet.set_column(f'{col}:{col}', width)

        for i, value in enumerate(column_names + rows):
            for j, val in enumerate(value):
                sheet.write_string(i, j, str(val))
        wb.close()

    def start_threading(self, inn: str, numkkt_rnm_fn_list: list) -> None:
        tread_list = []
        for i in range(self.threads):
            t = Thread(target=self.thread_job_rnm, args=(i, inn, numkkt_rnm_fn_list))
            t.start()
            tread_list.append(t)
        for i in range(self.threads):
            tread_list[i].join()

    def thread_job_rnm(self, num_thread: int, inn: str, numkkt_rnm_fn_list: list) -> None:
        for i in range(num_thread, len(numkkt_rnm_fn_list), self.threads):
            if not self.STOP_FLAG:
                try:
                    num_kkt = numkkt_rnm_fn_list[i][0]
                    rnm = numkkt_rnm_fn_list[i][1]
                    fn = numkkt_rnm_fn_list[i][2]
                    start_date = self.date_list[0]
                    end_date = self.date_list[1]
                    min_fd, max_fd = self.min_max_fd(rnm, fn, start_date, end_date)
                    if min_fd and max_fd:
                        min_fd, max_fd = int(min_fd), int(max_fd)
                        if self.download_json(inn, num_kkt, rnm, fn, min_fd, max_fd, i):
                            self.zipped(inn, rnm, fn, i)
                except Exception as ex:
                    self.exception = f"Ошибка возникла в функции {sys._getframe().f_code.co_name}\n" \
                                     f"Строка {str(sys.exc_info()[2].tb_lineno)}\n" \
                                     f"Текст ошибки '{str(ex)}'\n"
                    self.STOP_FLAG = True
                    exit()
            else:
                exit()

    @staticmethod
    def zipped(inn: str, rnm: str, fn: str, num: int) -> None:
        """Зипую папку с именем rnm.fn"""
        path = f'./{inn}/'
        file_dir = os.listdir(path)
        zip_name = f'{path}{rnm}.{fn}.zip'
        with zipfile.ZipFile(zip_name, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for file in file_dir:
                if file.endswith(f'@{num}.xlsx'):
                    os.renames(os.path.join(path, file), os.path.join(f'{path}{num}/', file))
                    os.renames(os.path.join(f'{path}{num}/', file),
                               os.path.join(f'{path}{num}/', file.replace(f'@{num}', '')))
                    file = file.replace(f'@{num}', '')
                    add_file = os.path.join(f'{path}{num}/', file)
                    zf.write(add_file, file)
                    os.remove(add_file)
                    os.rmdir(f'{path}{num}')

    @staticmethod
    def get_files() -> list:
        file_path = []
        file_list = []

        for root, dirs, files in os.walk('.'):
            file_path.append([os.path.join(root, file) for file in files])

        for folder in file_path:
            if folder:
                file_list += folder
        return file_list

    def final_zip(self) -> None:
        file_list = self.get_files()

        zip_name = f'../{self.request}.zip'

        with zipfile.ZipFile(zip_name, mode='w', compression=zipfile.ZIP_DEFLATED) as zipFile:
            for file in file_list:
                zipFile.write(file)
                file_dir = os.path.split(file)[0]
                os.remove(file)
                try:
                    os.removedirs(file_dir)
                except OSError:
                    pass
        try:
            os.chdir('..')
            os.removedirs(self.request)
        except OSError:
            pass

    def delete_unload(self) -> None:
        file_list = self.get_files()

        for file in file_list:
            file_dir = os.path.split(file)[0]
            os.remove(file)
            try:
                os.removedirs(file_dir)
            except OSError:
                pass
        try:
            os.chdir('..')
            os.removedirs(self.request)
        except OSError:
            pass


def main():
    uf = UnloadFns('request.txt')
    print('Запрос в БД')
    dict_inn_numkkt_rnm_fn = uf.get_job_dict()
    print('Начало выгрузки')
    for inn, numkkt_rnm_fn_list in dict_inn_numkkt_rnm_fn.items():
        if len(numkkt_rnm_fn_list) != 0:
            uf.start_threading(inn, numkkt_rnm_fn_list)
    if uf.STOP_FLAG:
        message = f"Во время выгрузки информации по заявке № {uf.request} произошла ошибка.\n" \
                  f"Просьба повторить попытку.\n" \
                  f"Ошибка:\n\n{uf.exception}"
        uf.delete_unload()
    else:
        message = f"Выгрузка по заявке № {uf.request} завершена успешно"
        uf.final_zip()
    print(message)


if __name__ == '__main__':
    main()
