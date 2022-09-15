from classes.Parser import Parser


if __name__ == "__main__":
    parser = Parser(
        parser_name="zakupki.mos.ru",
        is_sending_orders=False,
        append_base_path=False,
        is_sleeping=False
    )

    parser.start()
