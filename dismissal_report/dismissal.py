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
        connection = pg.connect(host=config["db_login"]["host"],
                                port=config["db_login"]["port"],
                                database=config["db_login"]["name"],
                                user=config["db_login"]["user"],
                                password=config["db_login"]["password"])

        cursor = connection.cursor()
        print(unload_date, "\tСоединение установлено.")
        print(config["db_login"]["table_name"])

        q1 = """
            create table if not exists {0} (k varchar(50));
            """.format(config["db_login"]["table_name"])

        print(q1)
        cursor.execute(q1)
        connection.commit()

    except (Exception, Error) as error:
        print("Ошибка подключения", error)

    else:
        cursor.close()
        connection.close()
        print("Подключение к БД закрыто.")

if __name__ == "__main__":
    main()