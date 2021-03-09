from threading import Thread, Lock
import requests
import psycopg2
from config import Config
import csv


class CsvJob:

    def open_csv(self, name: str):
        """Open CSV files"""
        file_list = []
        with open(name, "r", newline="") as file:
            reader = csv.reader(file)
            for row in reader:
                row = row[0].split(';')
                file_list.append(row)
        return file_list

    def write_file(self, name: str, mode: str, row: list):
        """Write to CSV file"""
        with open(name, mode, encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(row)

    def glue_csv(self, name_list: list, new_name: str):
        """Соединить несколько csv файлов"""
        for count_file, name in enumerate(name_list):
            for count_line, line in enumerate(self.open_csv(name)):
                if count_file > 0 and count_line == 0:
                    continue
                self.write_file(new_name, 'a', line)

class Connections:

    def to_elastic(self, data, index='*'):
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

    def __sql(func):
        def wrapper(self, request):
            connect_db = psycopg2.connect(
                database=Config.NAME_DATABASE_PROM,
                user=Config.USER_DB_PROM,
                password=Config.PASSWORD_DB_PROM,
                host=Config.HOST_DB_PROM,
                port=Config.PORT_DB_PROM
            )
            cursor = connect_db.cursor()
            return func(self, request, connect_db, cursor)

        return wrapper

    @__sql
    def sql_select(self, request, *args):
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

    @__sql
    def sql_update(self, request, *args):
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
