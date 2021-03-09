#!/usr/bin/env python3

from lib import Connections, CsvJob
import csv
import datetime as dt
import os
from math import ceil
import zipfile
from threading import Thread, Lock
import xlsxwriter
import sys
import pandas as pd


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
                rnm_string = f'and kkt.register_number_kkt in ({rnm_string})'
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

    def collect_rnm_inn(self):
        print('Запрос в базу данных...')
        """Формируем запрос SQL и получаем таблицу вида RNM - INN"""
        request = f"select kkt.factory_number_kkt, kkt.register_number_kkt, company.company_inn " \
                  f"from kkt inner join company on company." \
                  f"id=kkt.company_id  where company.company_inn in ({self.inn_string}) {self.rnm_string}"
        numkkt_rnm_inn_list = self.connect.sql_select(request)
        return numkkt_rnm_inn_list

    def collect_fn(self, rnm):
        """По полученым РНМ уточняем все установленные ФНы"""
        """в рамках указаных дат и по необходимым индексам"""
        query = """{
"size": 0,
"query" : {"bool" : {"must" : [
{"term" : {"requestmessage.kktRegId.raw" : "%s"}},
{"range" : {"requestmessage.dateTime" : {"gte" : "%s", "lte" : "%s" }}}
]}},
"aggs": {"fsIds": {"terms": {"field": "requestmessage.fiscalDriveNumber.raw","size": 500000}}}}""" % (rnm,
                                                                                                      self.date_list[0],
                                                                                                      self.date_list[1])
        fn_list = self.connect.to_elastic(query, 'receipt*,bso*,*_shift')['aggregations']['fsIds']['buckets']
        return fn_list

    def get_dict_inn_rnm_fn(self):
        numkkt_rnm_inn_list = self.collect_rnm_inn()
        if not numkkt_rnm_inn_list:
            print('Пар РНМ:ИНН не найдено.')
            exit()
        four_inn_numkkt_rnm_fn_dict = {}
        for num_kkt, rnm, inn in numkkt_rnm_inn_list:
            for fn in self.collect_fn(rnm):
                fn = fn['key']
                if four_inn_numkkt_rnm_fn_dict.get(inn):
                    four_inn_numkkt_rnm_fn_dict[inn].append((num_kkt, rnm, fn))
                else:
                    four_inn_numkkt_rnm_fn_dict[inn] = [(num_kkt, rnm, fn)]
        return four_inn_numkkt_rnm_fn_dict

    def min_max_fd(self, rnm, fn, start_date, end_date):
        """Получаем минимальный и максимальные ФД в периоде относительно РНМ и ФН"""
        stats_fd_request = '{"query" : {"bool" : {"filter" : {"bool" : {"must" : ' \
                           '[{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s" }},' \
                           '{"term" : {"requestmessage.kktRegId.raw" : "%s" }}, ' \
                           '{"range" : {"requestmessage.dateTime" : {"gte" : "%d", "lte" : "%d" }}}]}}}}, ' \
                           '"aggs" : {"stats" : { "stats" : { "field" : "requestmessage.fiscalDocumentNumber" }}}}' % (
                               fn, rnm, start_date, end_date)
        stats = self.connect.to_elastic(stats_fd_request,
                                        'receipt.*,bso,bso_correction,close_shift,open_shift')['aggregations']['stats']
        max_fd = stats['max']
        min_fd = stats['min']
        return min_fd, max_fd

    def get_information_on_receipt(self, receipt, num_kkt):

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
        nds0 = int(receipt.get('nds10', 0)) / 100 if receipt.get('nds0') else ''
        nds18118 = int(receipt.get('nds18118', 0)) / 100 if date_eq < date_to_20 and receipt.get('nds18118') else ''
        nds20120 = int(receipt.get('nds20120', 0)) / 100 if date_eq >= date_to_20 and receipt.get('nds20120') else ''
        nds10110 = int(receipt.get('nds10110', 0)) / 100 if receipt.get('nds10110') else ''
        ndsno = int(receipt.get('ndsNo', 0)) / 100 if receipt.get('ndsNo') else ''

        rec_list = []
        base = [receipt.get('user', ''),
                receipt.get('userInn', ''),
                receipt.get('retailPlace', ''),
                receipt.get('retailAddress', ''),
                '',  # внутреннее имя
                receipt.get('kktRegId', ''),
                num_kkt,  # брать из постгры
                receipt.get('fiscalDriveNumber', ''),
                sys_tax.get(receipt.get('appliedTaxationType'), ''),
                receipt.get('retailAddress', ''),
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
                receipt.get('provisionSum    ', 0) / 100,
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
                    '',  # единица измерения предмета расчета
                    '',  # код товарной номенклатуры
                    int(item.get('price', 0)) / 100,
                    nds.get(item.get('nds'), ''),
                    item.get('quantity', ''),
                    int(item.get('sum', 0)) / 100
                ]
                rec_list.append(base + lst)
        else:
            rec_list.append(base)
        return rec_list

    def download_json(self, inn, num_kkt, rnm, fn, min_fd, max_fd, num):
        """Основной скрипт выгрузки
        Формируется запрос согласно максимального и минимального ФД по РНМ:ФН
        Выгружаются по всем необходимым индексам
        Флаг необходим для запуска функции архивирования"""
        flag = False

        index_list = ['receipt.*', 'bso', 'bso_correction', 'close_shift', 'open_shift']

        delta = max_fd - min_fd
        iteration = ceil(delta / 10000)
        for type_fd in index_list:
            for _ in range(iteration):
                rec_list = []
                data = '{"from" : 0, "size" : 10000, "_source" : {"includes" : ["requestmessage.*"]}, ' \
                       '"query" : {"bool" : {"filter" : {"bool" : { "must" : ' \
                       '[{"term" : {"requestmessage.fiscalDriveNumber.raw" : "%s"}}, ' \
                       '{"term" : {"requestmessage.kktRegId.raw" : "%s"}},' \
                       '{"range" : {"requestmessage.fiscalDocumentNumber" : {"gte" : %d, "lte" : %d }}}]}}}}, ' \
                       '"sort" : [{ "requestmessage.fiscalDocumentNumber" : { "order" : "asc"}}]}' % \
                       (fn, rnm, min_fd, max_fd)
                receipts = self.connect.to_elastic(data, type_fd)['hits']['hits']

                for receipt in receipts:
                    rec_list += (self.get_information_on_receipt(receipt, num_kkt))
                    flag = True
                if rec_list:
                    self.write_csv(inn, rnm, fn, rec_list, num)
                min_fd += 10000
            """Возвращаем минимальное значение ФД"""
            min_fd = max_fd - delta
        return flag

    def write_xml(self, number_file, inn, rnm, fn, rows, num):
        column_names = [('Наименование налогоплательщика',
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
                         'Код товарной номенклатуры',
                         'Цена за единицу предмета расчета с учетом скидок и наценок',
                         'Размер НДС за единицу предмета расчета',
                         'Количество предмета расчета',
                         'Итоговая сумма предмета расчета'
                         )] + rows

        file_name = f'./{inn}/{rnm}.{fn}_{number_file}@{num}.xlsx'

        wb = xlsxwriter.Workbook(file_name)
        sheet = wb.add_worksheet()

        for i, value in enumerate(column_names):
            for j, val in enumerate(value):
                sheet.write(i, j, val)
        wb.close()

    def write_csv(self, inn, rnm, fn, rows, num):
        """Создание CSV файла для последующей перезаписью в Excel"""
        try:
            """В связи с тем, что несколько потоков пытаются создать папку, if не успевает. lock не вижу смысла"""
            if not os.path.exists(f"./{inn}/"):
                os.mkdir(f"./{inn}/")
        except FileExistsError:
            pass
        file_name = f'./{inn}/{rnm}.{fn}@{num}.csv'

        with open(file_name, 'a', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            for row in rows:
                writer.writerow(row)

    def csv_to_excel(self, inn, rnm, fn, num):
        """Способ гораздо быстрее, чем использование openpyxl"""
        file_name = f'./{inn}/{rnm}.{fn}@{num}.csv'

        chunks = pd.read_csv(file_name, chunksize=200000, iterator=True, header=None, engine='python')
        for number_file, rows in enumerate(chunks):
            rows = pd.DataFrame(rows).where(pd.notnull, '').values.tolist()
            self.write_xml(number_file + 1, inn, rnm, fn, rows, num)
        os.remove(file_name)

    def start_threading(self, inn, numkkt_rnm_fn_list):
        tread_list = []
        for i in range(self.threads):
            t = Thread(target=self.thread_job_rnm, args=(i, inn, numkkt_rnm_fn_list))
            t.start()
            tread_list.append(t)
        for i in range(self.threads):
            tread_list[i].join()

    def thread_job_rnm(self, num_thread, inn, numkkt_rnm_fn_list):
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
                            self.csv_to_excel(inn, rnm, fn, i)
                            self.zipped(inn, rnm, fn, i)
                except Exception as ex:
                    self.exception = f"Ошибка возникла в функции {sys._getframe().f_code.co_name}\n" \
                                     f"Строка {str(sys.exc_info()[2].tb_lineno)}\n" \
                                     f"Текст ошибки '{str(ex)}'\n"
                    self.STOP_FLAG = True
                    exit()
            else:
                exit()

    def zipped(self, inn, rnm, fn, num):
        """Зипую папку с именем rnm.fn.period"""
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

    def final_zip(self):
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

    def get_files(self) -> list:
        file_path = []
        file_list = []

        for root, dirs, files in os.walk('.'):
            file_path.append([os.path.join(root, file) for file in files])

        for folder in file_path:
            if folder:
                file_list += folder
        return file_list

    def delete_unload(self):
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
    dict_inn_numkkt_rnm_fn = uf.get_dict_inn_rnm_fn()
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
