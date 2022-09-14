# заходите на сервер - ознакомьтесь со структурой и инструкциями
# tenders_json.txt - это формат данных для тендеров
# Формат данных для отправки на апи.doc - передача в таком формате на апи идет
# https://zakupki.mos.ru/purchase/list - вот пример сайта который нужно распарсить
# данные могу разметить что в какое поле
# частота сбор раз в час за последние 60 минут

from Parser import Parser


if __name__ == "__main__":
    parser = Parser(
        parser_name="zakupki.mos.ru",
        is_sending_orders=False,
        append_base_path=False,
        is_sleeping=False
    )

    parser.start()
