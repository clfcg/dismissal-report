import psycopg2 as pg
from psycopg2 import Error
import xlsxwriter as xw

import os
from configparser import ConfigParser
import datetime


def main():
    config_path = 'dismissal_report/config/db_conf.ini'
    config = ConfigParser()
    config.read(config_path)

    export_url = config["path"]["unload_url"]
    data_from_db = take_data_from_db()
    file_name = datetime.datetime.now().strftime("%Y-%m-%d")
    values = data_from_db["records"]
    export_dir = f"{export_url}{data_from_db['max_date'][:7]}"
    titles = [
        "ОИВ", 
        "Организация", 
        "ИНН",
        "ФИО",
        "Подразделение",
        "Должность",
        "Дата увольнения",
        "Период"]
    
    #Создание каталога для выгрузки и выгрузка
    if not os.path.exists(export_dir):
        os.mkdir(export_dir)
        load_to_xls(titles, values, export_dir, file_name)
    else:
        load_to_xls(titles, values, export_dir, file_name)


def take_data_from_db():
    """
    Подключение к базе данных и экспорт данных по уволенным в список из
    кортежей.
    """
    try:
        config_path = 'dismissal_report/config/db_conf.ini'
        config = ConfigParser()
        config.read(config_path)
        connection = pg.connect(host=config["db_options"]["host"],
                                port=config["db_options"]["port"],
                                database=config["db_options"]["name"],
                                user=config["db_options"]["user"],
                                password=config["db_options"]["password"])

        cursor = connection.cursor()
        get_log("Соединение установлено.")

        #Проверка и создание таблицы с уволенными сотрудниками.
        query_check_table = """
            create table if not exists {0} (obl varchar(10) NOT NULL,
                                            oiv varchar(255) NOT NULL,
                                            org varchar(255) NOT NULL,
                                            inn varchar(20) NOT NULL,
                                            employee varchar(100) NULL,
                                            subdivision varchar(255) NULL,
                                            post varchar(255) NULL,
                                            date_dismiss date NULL,
                                            year_month varchar(20) NULL);
            """.format(config["db_options"]["table_name"])
        cursor.execute(query_check_table)

        #Выборка всех записей из таблицы.
        query_select_all_records = """
            SELECT 
                oiv, org, inn, 
                employee, subdivision, post, 
                date_dismiss, year_month
            FROM {0}
            ORDER BY oiv, org, date_dismiss
            """.format(config["db_options"]["table_name"])
        cursor.execute(query_select_all_records)
        records = cursor.fetchall()

        #Выборка максимальной даты
        query_select_max_date = """
            SELECT max(date_dismiss)
            FROM {0}
            """.format(config["db_options"]["table_name"])
        cursor.execute(query_select_max_date)
        max_date = cursor.fetchone()
        max_date = max_date[0].strftime("%Y-%m-%d")

        get_log("Данные получены из БД.")
        return {"records" : records, "max_date" : max_date}
    except (Exception, Error) as error:
        connection = None
        get_log(f"Ошибка подключения\n{error}")
    finally:
        if connection:
            connection.commit()
            cursor.close()
            connection.close()
            get_log(f"Подключение к базе данных закрыто.")


def load_to_xls(titles : list, values : list, exp_path : str, file_name: str):
    """
    Формирование .xlsx файла:
    -Форматирование ячеек.
    -Загрузка данных по заголовкам и содержанию ячеек.

    Передаются два списка, первый с заголовками, второй с данными.
    """
    wb = xw.Workbook(f"{exp_path}/{file_name}.xlsx")
    ws = wb.add_worksheet("Уволенные сотрудники")

    #Форматирование ячеек
    title_style = wb.add_format({"bold": True, 
        "font_name": "Times New Roman", 
        "font_size": 12})
    another_style = wb.add_format({"font_name": "Times New Roman",
        "font_size": 10})
    ws.autofilter("A1:H1")

    #Заголовки таблицы
    for col, data in enumerate(titles):
        ws.set_column(0, col, 25)
        ws.write(0, col, data, title_style)

    #Загрузка данных
    for row, tup in enumerate(values):
        for col, data in enumerate(tup):
            if not isinstance(data, datetime.date):
                ws.write(row+1, col, data, another_style)
            else:
                ws.write(row+1, col, data.strftime("%Y.%m.%d"), 
                    another_style)
    wb.close()
    get_log("Файл создан.")


def get_log(input_text : str):
    """
    Функция записи логов в файл.
    Принимает один аргумент - текст записи.
    """
    log_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if os.path.exists("Logs.log"):
        with open("Logs.log", "a", encoding="utf-8") as f:
            f.write(f"{log_datetime}>\t{input_text}\n")
            f.close()
    else:
        with open("Logs.log", "w", encoding="utf-8") as f:
            f.write(f"{log_datetime}>\t{input_text}\n")
            f.close()


if __name__ == "__main__":
    main()