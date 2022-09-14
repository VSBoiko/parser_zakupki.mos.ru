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
            self.add_logger_error("Error sending a request to site zakupki.mos")
            self.add_logger_error(err.response.content)
            return False

        self._to_sleep()

        self.write_json_file(json_filepath, response.json())
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
            print(f"[CUSTOMER_ALREADY_EXIST] - ({customer_id})")
            return False
        else:
            customer_url = self._get_customer_url(customer_id)
            customer_api_url = self._get_customer_api_url(customer_id)
            customer = self.get_customer(customer_api_url)

            if customer == {} or customer.get("httpStatusCode") == 404:
                self.add_logger_error(f"Error getting customer: {customer_api_url}")
                return False
            else:
                db.add_customer(
                    url=customer_url,
                    customer_id=customer_id,
                    customer_data=json.dumps(customer)
                )
                return True

    def add_order_to_db(self, db: ParserDb, order_type: str, order_id: str,
                        order_data: dict, customer_id: str) -> bool:
        db_order = db.get_order_by_order_id(order_id)
        if db_order:
            print(f"[ORDER_ALREADY_EXIST] - ({order_id})")
            return False
        else:
            item_url = self.get_item_url(order_type, order_id)
            item_api_url = self.get_item_api_url(order_type, order_id)
            item_detail = self.get_item(item_api_url)

            if item_detail == {} or item_detail.get("httpStatusCode") == 404:
                self.add_logger_error(f"Error getting item_detail: {item_url}")
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
                return True

    def add_data_to_db(self, json_filepath: str, db: ParserDb):
        data = self.read_json_file(json_filepath)
        count_all_item = data["count"]
        count = 0
        for item in data["items"]:
            count += 1
            iter_info = f"#{count} / {count_all_item}"
            print(f"{iter_info}: [START] - {item.get('name')} ({item.get('number')})")

            customer_id = item.get('customers')[0].get('id')
            self.add_customer_to_db(db, customer_id)

            item_type = self.get_item_type(item)
            item_id = self.get_item_id(item_type, item)
            if self.check_order(item):
                self.add_order_to_db(db, item_type, item_id, item, customer_id)

    def check_order(self, order):
        dont_send = [
            "4122001"  # test order
        ]

        if order.get("number") in dont_send:
            return False
        elif len(order.get('customers')) == 0:
            print(f"[ORDER MISSED] - order '{order.get('name')}' doesn`t have customers")
            self.add_logger_error(f"Order doesn`t have customers: '{order.get('name')}'")
            return False

        return True

    def send_orders_from_db(self, db: ParserDb):
        orders = db.get_unsent_orders()
        count_all_orders = len(orders)
        count, count_send, count_send_error = 0, 0, 0
        for order in orders:
            count += 1
            iter_info = f"#{count} / {count_all_orders}"

            order_data = order["order_data"]
            print(f"{iter_info}: [START] - {order_data.get('name')} ({order_data.get('number')})")

            order_type = order["order_type"]
            customer = db.get_customer_by_customer_id(order["customer_id"])
            order = {}
            try:
                if order_type == self.__auction_type:
                    order = self.formatted_order_auction(order, customer)
                elif order_type == self.__need_type:
                    order = self.formatted_order_need(order, customer)
                elif order_type == self.__tender_type:
                    # order = self.formatted_order_tender(order_data, order_detail, order_url, customer)
                    order = {}
            except Exception as err:
                self.add_logger_error(f"Error creating order to send: {order['url']}")
                self.add_logger_error(err)

            if order:
                if self._send_orders([order]):
                    db.update_send_on_success(order["order_id"])
                    count_send += 1
                else:
                    count_send_error += 1
            else:
                print(f"{iter_info}: [ORDER IS EMPTY] - ({order_data.get('number')}) {order_data.get('name')} ")
                count_send_error += 1

        return {
            "new_orders": count_send,
            "errors": count_send_error,
        }

    def formatted_order_need(self, order, customer):
        order_data = order["order_data"]
        order_detail = order["order_detail"]
        order_url = order["order_url"]
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
        order_data = order["order_data"]
        order_detail = order["order_detail"]
        order_url = order["order_url"]
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
            self.add_logger_error("Error when sending a request to the site zakupki.mos")
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
            self.add_logger_error("Error sending a request to site zakupki.mos")
            self.add_logger_error(err.response.content)
            return {}

        self._to_sleep()

        return response.json()

    def start(self):
        time_start = datetime.datetime.now()
        print(f"[SCRIPT START] - {time_start.strftime('%d.%m.%Y, %H:%M:%S')}")

        self.add_logger_info("Script start")

        result = {
            "new_orders": -1,
            "errors": -1
        }

        mos_url = "https://old.zakupki.mos.ru/api/Cssp/Purchase/Query?queryDto=%7B%22filter%22%3A%7B%22auctionSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B19000002%5D%7D%2C%22needSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B20000002%5D%7D%2C%22tenderSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B5%5D%7D%7D%2C%22order%22%3A%5B%7B%22field%22%3A%22relevance%22%2C%22desc%22%3Atrue%7D%5D%2C%22withCount%22%3Atrue%2C%22skip%22%3A0%7D"
        data_filepath = "./data.json"
        if os.path.exists(data_filepath):
            print(f"[ERROR] - {data_filepath} already exist")
            self.add_logger_error(f"{data_filepath} already exist, new orders doesn`t loaded")
            # os.remove(f"{data_filepath}")
            # logger.info(f"{data_filepath} deleted")
        else:
            self.create_data_file(mos_url, data_filepath)

            db = ParserDb("zakupkimos.db")
            self.add_data_to_db(data_filepath, db)
            result = self.send_orders_from_db(db)
            os.remove(f"{data_filepath}")

        time_finish = datetime.datetime.now()
        print(f"[SCRIPT FINISH] - {time_finish.strftime('%d.%m.%Y, %H:%M:%S')}")

        self.add_logger_info(f"Script finish. "
                             f"{result.get('new_orders')} new orders sent. "
                             f"{result.get('errors')} orders with errors.")

    @staticmethod
    def _get_document_url(document_id: str) -> str:
        return f"https://zakupki.mos.ru/newapi/api/FileStorage/Download?id={document_id}"
