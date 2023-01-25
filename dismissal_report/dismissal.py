import psycopg2 as pg
from psycopg2 import Error

from configparser import ConfigParser


def main():
    connection_to_db()

def connection_to_db():
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
        query = """
        SELECT version();
        """
        cursor.execute(query)
        print("Версия PostgreSQL:")
        print(cursor.fetchone())

    except (Exception, Error) as error:
        print("Ошибка подключения", error)

    else:
        cursor.close()
        connection.close()
        print("Подключение к БД закрыто.")

if __name__ == "__main__":
    main()