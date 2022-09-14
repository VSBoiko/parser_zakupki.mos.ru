import json
import os
import random
import sys
import logging
from time import sleep


class BaseParser:
    def __init__(self, parser_name: str,
                 is_logging: bool = True,
                 is_sleeping: bool = True,
                 is_sending_orders: bool = True,
                 append_base_path: bool = True):
        self._parser_name = parser_name
        self._is_logging = is_logging
        self._is_sleeping = is_sleeping
        self._is_sending_orders = is_sending_orders
        self._append_base_path = append_base_path
        self.__headers: dict
        self.__logger: logging.Logger

        if self._append_base_path:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            base_path = os.path.dirname(current_dir)
            sys.path.append(base_path)

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0"
        }
        self._set_headers(headers)

        log_filename = "errors.log"
        log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - " \
                     f"(%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
        self._set_logger(log_filename, log_format)

    def add_logger_error(self, content):
        if self._is_logging:
            self._get_logger().error(content)

    def add_logger_info(self, content):
        if self._is_logging:
            self._get_logger().info(content)

    @staticmethod
    def read_json_file(filename: str):
        with open(filename, "r") as file:
            return json.load(file)

    @staticmethod
    def write_json_file(filename: str, data):
        with open(filename, "w") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

    def _get_headers(self) -> dict:
        return self.__headers

    def _get_logger(self) -> logging.Logger:
        return self.__logger

    def _get_parser_name(self):
        return self._parser_name

    def _send_to_api(self, data: dict):
        path = "/home/manage_report"
        if os.path.exists(path) and self._is_sending_orders:
            sys.path.append(path)
            from Send_report.Utils import send_to_api

            send_to_api(data)

    def _send_orders(self, orders: list) -> bool:
        data = {
            'name': self._get_parser_name(),
            'data': orders
        }
        try:
            self._send_to_api(data)
            self.add_logger_info(f"Sent {len(orders)} orders to API")
            print(f"[SEND ORDERS] - {len(orders)} orders")
            return True
        except Exception as err:
            self.add_logger_error("Error while sending orders")
            self.add_logger_error(err)
            print(f"[SEND ORDERS ERROR] - {len(orders)} orders")
            return False

    def _set_headers(self, new_headers: dict):
        self.__headers = new_headers

    def _set_logger(self, log_filename: str, log_format: str):
        logging.basicConfig(filename=log_filename, level=logging.INFO, format=log_format)
        self.__logger = logging.getLogger(__name__)

    def _to_sleep(self, start: int = 2, stop: int = 4):
        if self._is_sleeping:
            sleep(random.randrange(start, stop))
