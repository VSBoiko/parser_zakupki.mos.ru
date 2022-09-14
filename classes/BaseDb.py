import sqlite3


class BaseDb:
    def __init__(self, db_name: str):
        self._db_name = db_name

    def create_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_name)
        return connection

    def get_all_from_db(self, query) -> list:
        connection = self.create_connection()
        result = [row for row in connection.execute(query)]
        return result

    def get_db_name(self):
        return self._db_name

    def write_data_to_db(self, query, data) -> bool:
        connection = self.create_connection()
        try:
            with connection:
                connection.executemany(query, data)
        except sqlite3.IntegrityError as err:
            # print('Возникла ошибка: ', err)
            return False
        else:
            # print('Запись данных прошла успешно')
            return True
