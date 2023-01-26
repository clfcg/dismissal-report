import psycopg2 as pg
from psycopg2 import Error

from configparser import ConfigParser
from datetime import datetime


def main():
    connection_to_db()

def connection_to_db():
    unload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                                            date_dismissal date NULL,
                                            year_month varchar(20) NULL);
            """.format(config["db_options"]["table_name"])
        cursor.execute(query_check_table)

        #Выборка всех записей из таблицы.
        query_select_all_records = """
            SELECT * 
            FROM {0}
            ORDER BY oiv, org, employee
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