import datetime
import json
import os
import requests

from BaseParser import BaseParser
from ParserDb import ParserDb


class Parser(BaseParser):
    __auction_type = "auction"
    __need_type = "need"
    __tender_type = "tender"

    def create_data_file(self, url: str, json_filepath: str) -> bool:
        try:
            response = requests.get(
                url,
                headers=self._get_headers()
            )
        except requests.exceptions.RequestException as err:
            print("[ERROR] Ошибка при запросе на получении списка заказов с сайта zakupki.mos")
            self.add_logger_error("Ошибка при запросе на получении списка заказов с сайта zakupki.mos")
            self.add_logger_error(err.response.content)
            return False

        self._to_sleep()

        self.write_json_file(json_filepath, response.json())
        print(f"[FILE CREATED] Файл {json_filepath} со списком заказов успешно создан")
        self.add_logger_info(f"Файл {json_filepath} со списком заказов успешно создан")

        return True

    @staticmethod
    def _get_customer_api_url(customer_id: str) -> str:
        return f"https://zakupki.mos.ru/newapi/api/CompanyProfile/" \
               f"GetByCompanyId?companyId={customer_id}"

    @staticmethod
    def _get_customer_url(customer_id: str) -> str:
        return f"https://zakupki.mos.ru/companyProfile/customer/{customer_id}"

    def get_item_type(self, item: dict) -> str:
        if item.get("auctionId"):
            return self.__auction_type
        elif item.get("needId"):
            return self.__need_type
        elif item.get("tenderId"):
            return self.__tender_type

    def get_item_id(self, item_type: str, item: dict) -> str:
        if item_type == self.__auction_type:
            return str(item['auctionId'])
        elif item_type == self.__need_type:
            return str(item['needId'])
        elif item_type == self.__tender_type:
            return str(item['tenderId'])

    def get_item_url(self, item_type: str, item_id: str) -> str:
        if item_type == self.__auction_type:
            return f"https://zakupki.mos.ru/auction/{item_id}"
        elif item_type == self.__need_type:
            return f"https://zakupki.mos.ru/need/{item_id}"
        elif item_type == self.__tender_type:
            return f"https://old.zakupki.mos.ru/#/tenders/{item_id}"

    def get_item_api_url(self, item_type: str, item_id: str) -> str:
        if item_type == self.__auction_type:
            return f"https://zakupki.mos.ru/newapi/api/" \
                   f"{item_type.capitalize()}/Get?{item_type}Id={item_id}"
        elif item_type == self.__need_type:
            return f"https://zakupki.mos.ru/newapi/api/" \
                   f"{item_type.capitalize()}/Get?{item_type}Id={item_id}"
        elif item_type == self.__tender_type:
            return f"https://old.zakupki.mos.ru/api/Cssp/" \
                   f"{item_type.capitalize()}/GetEntity?id={item_id}"

    def add_customer_to_db(self, db: ParserDb, customer_id: str) -> bool:
        db_customer = db.get_customer_by_customer_id(customer_id)
        if db_customer:
            # print(f"[ALREADY EXIST] Заказчик уже существует в БД - ({customer_id})")
            # self.add_logger_info(f"Заказчик уже существует в БД - ({customer_id})")
            return False
        else:
            customer_url = self._get_customer_url(customer_id)
            customer_api_url = self._get_customer_api_url(customer_id)
            customer = self.get_customer(customer_api_url)

            if customer == {} or customer.get("httpStatusCode") == 404:
                print(f"[ERROR] Ошибка при получении заказчика: {customer_url}")
                self.add_logger_error(f"Ошибка при получении заказчика: {customer_url}")
                return False
            else:
                db.add_customer(
                    url=customer_url,
                    customer_id=customer_id,
                    customer_data=json.dumps(customer)
                )
                print(f"[SUCCESS] Заказчик {customer_id} успешно добавлен в БД")
                self.add_logger_info(f"Заказчик {customer_id} успешно добавлен в БД")
                return True

    def add_order_to_db(self, db: ParserDb, order_type: str, order_id: str,
                        order_data: dict, customer_id: str) -> bool:
        db_order = db.get_order_by_order_id(order_id)
        if db_order:
            # print(f"[ALREADY EXIST] Заказ уже существует в БД - ({order_id})")
            # self.add_logger_info(f"Заказ уже существует в БД - ({order_id})")
            return False
        else:
            item_url = self.get_item_url(order_type, order_id)
            item_api_url = self.get_item_api_url(order_type, order_id)
            item_detail = self.get_item(item_api_url)

            if item_detail == {} or item_detail.get("httpStatusCode") == 404:
                print(f"[ERROR] Ошибка при получении детальной инф-ции о заказе: {item_url}")
                self.add_logger_error(f"Ошибка при получении детальной инф-ции о заказе: {item_url}")
                return False
            else:
                db.add_order(
                    url=item_url,
                    order_type=order_type,
                    order_id=order_id,
                    order_data=json.dumps(order_data),
                    order_detail=json.dumps(item_detail),
                    customer_id=customer_id
                )
                print(f"[SUCCESS] Заказ {order_id} успешно добавлен в БД")
                self.add_logger_info(f"Заказ {order_id} успешно добавлен в БД")
                return True

    def add_data_to_db(self, json_filepath: str, db: ParserDb):
        data = self.read_json_file(json_filepath)
        count_all_item = data["count"]
        count = 0
        print("[START] Начало добавления заказов в БД")
        for item in data["items"][0:10]:
            count += 1
            iter_info = f"#{count} / {count_all_item}"
            print(f"{iter_info}: [ORDER] Заказ ({item.get('number')}) {item.get('name')}")
            # self.add_logger_info(f"Заказ ({item.get('number')}) {item.get('name')}")

            customer_id = item.get('customers')[0].get('id')
            self.add_customer_to_db(db, customer_id)

            item_type = self.get_item_type(item)
            item_id = self.get_item_id(item_type, item)
            if self.check_order(item):
                self.add_order_to_db(db, item_type, item_id, item, customer_id)
        print("[FINISH] Конец добавления заказов в БД\n")

    def check_order(self, order):
        dont_send = [
            "4122001"  # test order
        ]

        if order.get("number") in dont_send:
            return False
        elif len(order.get('customers')) == 0:
            print(f"[ERROR] Заказ не имеет заказчика: number = '{order.get('number')}'")
            self.add_logger_error(f"Заказ не имеет заказчика: number = '{order.get('number')}'")
            return False

        return True

    def send_orders_from_db(self, db: ParserDb):
        orders = db.get_unsent_orders()
        count_all_orders = len(orders)
        count, count_send, count_send_error = 0, 0, 0
        print("[START] Начало отправки заказов по API")

        if count_all_orders == 0:
            print("[INFO] Новых заказов нет")
            self.add_logger_info("Новых заказов нет")

        for order in orders:
            count += 1
            iter_info = f"#{count} / {count_all_orders}"

            order_data = json.loads(order.get("order_data"))
            print(f"{iter_info}: [ORDER] Заказ ({order_data.get('number')}) {order_data.get('name')}")

            order_type = order.get("order_type")
            customer = db.get_customer_by_customer_id(order.get("customer_id"))
            formatted_order = {}
            try:
                if order_type == self.__auction_type:
                    formatted_order = self.formatted_order_auction(order, customer)
                elif order_type == self.__need_type:
                    formatted_order = self.formatted_order_need(order, customer)
                elif order_type == self.__tender_type:
                    # order = self.formatted_order_tender(order_data, order_detail, order_url, customer)
                    formatted_order = {}
            except Exception as err:
                print(f"[ERROR] Ошибка при создании заказа для отправки по API: {order.get('url')}")
                self.add_logger_error(f"Ошибка при создании заказа для отправки по API: {order.get('url')}")
                self.add_logger_error(err)
            if formatted_order:
                if self._send_orders([formatted_order]):
                    db.update_send_on_success(order.get("order_id"))
                    print(f"[SUCCESS] Заказ успешно отправлен по API: {order.get('url')}")
                    self.add_logger_info(f"Заказ успешно отправлен по API: {order.get('url')}")
                    count_send += 1
                else:
                    print(f"[ERROR] Заказ не отправлен по API: {order.get('url')}")
                    self.add_logger_error(f"Заказ не отправлен по API: {order.get('url')}")
                    count_send_error += 1
            else:
                print(f"[EMPTY ORDER] Заказ пустой: {order.get('url')}")
                self.add_logger_info(f"Заказ пустой: {order.get('url')}")
                count_send_error += 1

        print("[FINISH] Конец отправки заказов по API\n")

        return {
            "new_orders": count_send,
            "errors": count_send_error,
        }

    def formatted_order_need(self, order, customer) -> dict:
        order_data = json.loads(order.get("order_data"))
        order_detail = json.loads(order.get("order_detail"))
        order_url = order.get("url")

        customer_detail = json.loads(customer.get("customer_data"))
        result = {
            "fz": "ЗМО",
            "purchaseNumber": order_data.get("number"),
            "url": order_url,
            "title": order_data.get("name"),
            "purchaseType": "Закупка по потребности",
            "procedureInfo": {
                "endDate": order_data.get("endDate")
            },
            "lots": [{
                "price": order_detail.get("nmck"),
                "customerRequirements": [{
                    "kladrPlaces": [{
                        "deliveryPlace": order_detail.get("deliveryPlace"),
                    }]
                }],
            }],
            "ETP": {
                "name": "zakupki.mos.ru"
            },
            "attachments": [{
                "docDescription": doc.get("name"),
                "url": self._get_document_url(doc.get('id'))
            } for doc in order_detail.get("files")],
            "type": 2
        }

        # customer
        if customer_detail.get("company").get("factAddress"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"factAddress": customer_detail.get("company").get("factAddress")})

        if customer_detail.get("company").get("inn"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"inn": customer_detail.get("company").get("inn")})

        if customer_detail.get("company").get("kpp"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"kpp": customer_detail.get("company").get("kpp")})

        # contactPerson
        contact_name = order_detail.get("contactPerson").split() if order_detail.get("contactPerson") else ["", ""]
        if len(contact_name) == 1:
            contact_name.append("")

        if contact_name[0]:
            if "contactPerson" not in result:
                result.update({"contactPerson": {}})
            result["contactPerson"].update({"lastName": contact_name[0]})

        if contact_name[1]:
            if "contactPerson" not in result:
                result.update({"contactPerson": {}})
            result["contactPerson"].update({"firstName": contact_name[1]})

        if order_detail.get("contactEmail"):
            if "contactPerson" not in result:
                result.update({"contactPerson": {}})
            result["contactPerson"].update({"contactEMail": order_detail.get("contactEmail")})

        if order_detail.get("contactPhone"):
            if "contactPerson" not in result:
                result.update({"contactPerson": {}})
            result["contactPerson"].update({"contactPhone": order_detail.get("contactPhone")})

        return result

    def formatted_order_auction(self, order, customer) -> dict:
        order_data = json.loads(order["order_data"])
        order_detail = json.loads(order["order_detail"])
        order_url = order.get("url")
        deliveries = order_detail.get("deliveries")[0]

        result = {
            "fz": "ЗМО",
            "purchaseNumber": order_data.get("number"),
            "url": order_url,
            "title": order_data.get("name"),
            "purchaseType": "Котировочная сессия",
            "procedureInfo": {
                "endDate": order_data.get("endDate")
            },
            "lots": [{
                "price": order_detail.get("startCost"),
                "customerRequirements": [{
                    "kladrPlaces": deliveries.get("deliveryPlace"),
                    "obesp_i": order_detail.get("contractGuaranteeAmount")
                }],
            }],
            "ETP": {
                "name": "zakupki.mos.ru"
            },
            "attachments": [{
                "docDescription": doc.get("name"),
                "url": self._get_document_url(doc.get('id'))
            } for doc in order_detail.get("files")],
            "type": 2
        }

        # customer
        if customer.get("company").get("factAddress"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"factAddress": customer.get("company").get("factAddress")})

        if customer.get("company").get("inn"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"inn": customer.get("company").get("inn")})

        if customer.get("company").get("kpp"):
            if "customer" not in result:
                result.update({"customer": {}})
            result["customer"].update({"kpp": customer.get("company").get("kpp")})

        return result

    def formatted_order_tender(self, item, detail, item_url, customer):
        pass

    def get_customer(self, customer_url: str) -> dict:
        try:
            response = requests.get(
                url=customer_url,
                headers=self._get_headers()
            )
        except requests.exceptions.RequestException as err:
            self.add_logger_error("Ошибка при отправке запроса на получение инф-ции о заказчике")
            self.add_logger_error(err.response.content)
            return {}

        self._to_sleep()

        return response.json()

    def get_item(self, detail_url: str) -> dict:
        try:
            response = requests.get(
                url=detail_url,
                headers=self._get_headers()
            )
        except requests.exceptions.RequestException as err:
            self.add_logger_error("Ошибка при отправке запроса на получение детальной инф-ции о заказе")
            self.add_logger_error(err.response.content)
            return {}

        self._to_sleep()

        return response.json()

    def start(self):
        time_start = datetime.datetime.now()
        print(f"[PARSER] Парсер начал работу в {time_start.strftime('%d.%m.%Y, %H:%M:%S')}")

        self.add_logger_info("Парсер начал работу")

        result = {
            "new_orders": -1,
            "errors": -1
        }

        mos_url = "https://old.zakupki.mos.ru/api/Cssp/Purchase/Query?queryDto=%7B%22filter%22%3A%7B%22auctionSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B19000002%5D%7D%2C%22needSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B20000002%5D%7D%2C%22tenderSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B5%5D%7D%7D%2C%22order%22%3A%5B%7B%22field%22%3A%22relevance%22%2C%22desc%22%3Atrue%7D%5D%2C%22withCount%22%3Atrue%2C%22skip%22%3A0%7D"
        data_filepath = "./data.json"
        if os.path.exists(data_filepath):
            print(f"[ERROR] Файл {data_filepath} существует, новые заказы не могут быть загруженыt")
            self.add_logger_error(f"Файл {data_filepath} существует, новые заказы не могут быть загружены")
            # os.remove(f"{data_filepath}")
            # logger.info(f"{data_filepath} deleted")
        else:
            self.create_data_file(mos_url, data_filepath)

            db = ParserDb("zakupkimos.db")
            db.create_table_orders()
            db.create_table_customers()
            self.add_data_to_db(data_filepath, db)
            result = self.send_orders_from_db(db)
            os.remove(f"{data_filepath}")
            print(f"[INFO] Файл {data_filepath} успешно удален")
            self.add_logger_info(f"Файл {data_filepath} успешно удален")

        time_finish = datetime.datetime.now()
        print(f"[PARSER] Парсер закончил работу в {time_finish.strftime('%d.%m.%Y, %H:%M:%S')}. "
              f"{result.get('new_orders')} новых заказов отправлено. "
              f"{result.get('errors')} заказов с ошибкой.")

        self.add_logger_info(f"Парсер закончил работу. "
                             f"{result.get('new_orders')} новых заказов отправлено. "
                             f"{result.get('errors')} заказов с ошибкой.")

    @staticmethod
    def _get_document_url(document_id: str) -> str:
        return f"https://zakupki.mos.ru/newapi/api/FileStorage/Download?id={document_id}"
