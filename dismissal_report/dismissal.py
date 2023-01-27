import psycopg2 as pg
from psycopg2 import Error
import xlsxwriter as xw

from configparser import ConfigParser
import datetime


def main():
    titles = [
        "ОИВ", 
        "Организация", 
        "ИНН",
        "ФИО",
        "Подразделение",
        "Должность",
        "Дата увольнения",
        "Период"]
    values = take_data_from_db()
    load_to_xls(titles, values)


def load_to_xls(titles : list, values : list):
    """
    Формирование .xlsx файла:
    -Форматирование ячеек.
    -Загрузка данных по заголовкам и содержанию ячеек.

    Передаются два списка, первый с заголовками, второй с данными.
    """
    wb = xw.Workbook("temp.xlsx")
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


def take_data_from_db():
    """
    Подключение к базе данных и экспорт данных по уволенным в список из
    кортежей.
    """
    unload_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        print(f"{unload_date}\t>\tСоединение установлено.")

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
        print(f"{unload_date}\t>\tДанные получены из БД.")
        return records
    except (Exception, Error) as error:
        connection = None
        print(f"{unload_date}\t>\tОшибка подключения\n{error}")
    finally:
        if connection:
            connection.commit()
            cursor.close()
            connection.close()
            print(f"{unload_date}\t>\tПодключение к БД закрыто.")

if __name__ == "__main__":
    main()