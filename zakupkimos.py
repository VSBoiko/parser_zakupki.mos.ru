import datetime
import json
import os
import random
import sys
from time import sleep

import logging

import requests

# currentdir = os.path.dirname(os.path.realpath(__file__))
# base_path = os.path.dirname(currentdir)
# sys.path.append(base_path)
# sys.path.append('/home/manage_report')
# from Send_report.Utils import send_to_api

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "User-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:104.0) Gecko/20100101 Firefox/104.0"
}

_log_format = f"%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
logging.basicConfig(filename='errors.log', level=logging.INFO, format=_log_format)
logger = logging.getLogger(__name__)


def create_data_file(url: str, json_filepath: str) -> bool:
    try:
        response = requests.get(
            url,
            headers=headers
        )
    except requests.exceptions.RequestException as err:
        logger.error("Error when sending a request to the site zakupki.mos")
        logger.error(err.response.content)
        return False

    # sleep(random.randrange(1, 2))

    with open(json_filepath, "w") as json_file:
        json.dump(response.json(), json_file, indent=4, ensure_ascii=False)

    return True


def get_orders(json_filepath: str) -> dict:
    with open(json_filepath, "r") as json_file:
        json_data = json.load(json_file)

    # errors = []
    orders = []
    dirs = [
        "auction",
        "need",
        "tender",
        "customers"
    ]

    for dir in dirs:
        if not os.path.exists(f"./data/{dir}"):
            os.mkdir(f"./data/{dir}")
            print(f"[CREATE DIR] - ./data/{dir}")

    all_item_count = json_data["count"]
    count = 1
    count_send = 0
    count_send_error = 0
    for item in json_data["items"]:
        if item["number"] == "4122001":
            count += 1
            continue
        elif len(item.get('customers')) == 0:
            print(f"[ORDER MISSED] - order '{item.get('name')}' doesn`t have customers")
            logger.error(f"Order '{item.get('name')}' doesn`t have customers")
            count += 1
            continue

        iter_info = f"#{count} / {all_item_count}"
        print(f"{iter_info}: [START] - {item.get('name')} ({item.get('number')})")

        if item.get("auctionId"):
            type = "auction"
            id = str(item['auctionId'])
            item_url = f"https://zakupki.mos.ru/auction/{id}"
        elif item.get("needId"):
            type = "need"
            id = str(item['needId'])
            item_url = f"https://zakupki.mos.ru/need/{id}"
        elif item.get("tenderId"):
            type = "tender"
            id = str(item['tenderId'])
            item_url = f"https://old.zakupki.mos.ru/#/tenders/{id}"

        item_filename = f"{item['number']}.json"

        if os.path.exists(f"./data/{type}/{item_filename}"):
            print(f"{iter_info}: [ORDER_ALREADY_SENT] - {item.get('name')} ({item.get('number')})")
            count += 1
            continue

        item_detail = get_item_detail(item_filename, type, item, id, iter_info)

        if item_detail == {} or item_detail.get("httpStatusCode") == 404:
            os.remove(f"./data/{type}/{item_filename}")
            logger.error(f"Error getting item_detail - {item_url}")
            count += 1
            continue

        customer_id = item.get('customers')[0].get('id')
        customer_filename = f"{customer_id}.json"
        customer = get_customer(customer_filename, customer_id, iter_info)

        if customer == {} or customer.get("httpStatusCode") == 404:
            os.remove(f"./data/customers/{customer_filename}")
            os.remove(f"./data/{type}/{item_filename}")
            logger.error(f"Error getting customer - {item_url}")
            count += 1
            continue

        order = {}
        try:
            if type == "auction":
                order = get_auction(item, item_detail, item_url, customer)
            elif type == "need":
                order = get_need(item, item_detail, item_url, customer)
            elif type == "tender":
                order = get_tender(item, item_detail, item_url, customer)
        except Exception as err:
            logger.error(f"Error getting customer - {item_url}")
            logger.error(err)
            # errors.append(f"{iter_info}: [ERROR] - {item.get('number')}: {item_url}")

        if order != {}:
            orders.append(order)
            logger.info(f"Order [{item.get('number')}] {item.get('name')} was added to shipping list")
            print(f"{iter_info}: [ORDER ADDED TO SHIPPING LIST] - {item.get('name')} ({item.get('number')})")
            count_send += 1
        else:
            print(f"{iter_info}: [ORDER NOT ADDED TO SHIPPING LIST] - {item.get('name')} ({item.get('number')})")
            count_send_error += 1

        if count % 5000 == 0:
            data = {
                'name': 'zakupki.mos.ru',
                'data': orders
            }
            # send_to_api(data)
            logger.info(f"Sent {len(orders)} orders to API")
            print(f"[SEND ORDERS] - {len(orders)} orders")
            orders.clear()

        count += 1
        sleep(random.randrange(2, 4))

    if len(orders) > 0:
        data = {
            'name': 'zakupki.mos.ru',
            'data': orders
        }
        # send_to_api(data)
        logger.info(f"Sent {len(orders)} orders to API")
        print(f"[SEND ORDERS] - {len(orders)} orders")
        orders.clear()

    # with open(f"./result.json", "w") as json_file:
    #     json.dump(orders, json_file, indent=4, ensure_ascii=False)

    # with open(f"./errors.json", "w") as json_file:
    #     json.dump(errors, json_file, indent=4, ensure_ascii=False)

    return {
        "new_orders": count_send,
        "errors": count_send_error,
    }


def get_item_detail(filename: str, type: str, item: dict, id:str, iter_info: str) -> dict:
    if type == "auction":
        detail_url = f"https://zakupki.mos.ru/newapi/api/{type.capitalize()}/Get?{type}Id={id}"
    elif type == "need":
        detail_url = f"https://zakupki.mos.ru/newapi/api/{type.capitalize()}/Get?{type}Id={id}"
    elif type == "tender":
        detail_url = f"https://old.zakupki.mos.ru/api/Cssp/{type.capitalize()}/GetEntity?id={id}"

    if not os.path.exists(f"./data/{type}/{filename}"):
        try:
            response = requests.get(
                url=detail_url,
                headers=headers
            )
        except requests.exceptions.RequestException as err:
            logger.error("Error when sending a request to the site zakupki.mos")
            logger.error(err.response.content)
            return {}

        # sleep(random.randrange(1, 2))
        with open(f"./data/{type}/{filename}", "w") as json_file:
            item_detail = response.json()
            json.dump(item_detail, json_file, indent=4, ensure_ascii=False)
            print(f"{iter_info}: [ADD JSON] - {item.get('name')} ({item.get('number')})")
    else:
        with open(f"./data/{type}/{filename}", "r") as json_file:
            item_detail = json.load(json_file)

    return item_detail


def get_customer(filename: str, customer_id: str, iter_info: str) -> dict:
    if not os.path.exists(f"./data/customers/{filename}"):
        customer_url = f"https://zakupki.mos.ru/newapi/api/CompanyProfile/" \
                       f"GetByCompanyId?companyId={customer_id}"
        try:
            response = requests.get(
                url=customer_url,
                headers=headers
            )
        except requests.exceptions.RequestException as err:
            logger.error("Error when sending a request to the site zakupki.mos")
            logger.error(err.response.content)
            return {}

        # sleep(random.randrange(1, 2))

        with open(f"./data/customers/{filename}", "w") as json_file:
            customer = response.json()
            json.dump(customer, json_file, indent=4, ensure_ascii=False)
            print(f"{iter_info}: [ADD CUSTOMER] - {customer_id}")
    else:
        with open(f"./data/customers/{filename}", "r") as json_file:
            customer = json.load(json_file)

    return customer


def get_need(item, detail, item_url, customer):
    item_contact_name = detail.get("contactPerson").split() if detail.get("contactPerson") else ["", ""]
    if len(item_contact_name) == 1:
        item_contact_name.append("")

    result = {
        "fz": "ЗМО",
        "purchaseNumber": item.get("number"),
        "url": item_url,
        "title": item.get("name"),
        "purchaseType": "Закупка по потребности",
        "procedureInfo": {
            "endDate": item.get("endDate")
        },
        "lots": [{
            "price": detail.get("nmck"),
            "customerRequirements": [{
                "kladrPlaces": [{
                    "deliveryPlace": detail.get("deliveryPlace"),
                }]
            }],
        }],
        "ETP": {
            "name": "zakupki.mos.ru"
        },
        "attachments": [{
            "docDescription": doc.get("name"),
            "url": f"https://zakupki.mos.ru/newapi/api/FileStorage/Download?id={doc.get('id')}"
        } for doc in detail.get("files")],
        "type": 2
    }

    # customer
    if customer.get("company").get("factAddress"):
        result["customer"].update({"factAddress": customer.get("company").get("factAddress")})

    if customer.get("company").get("inn"):
        result["customer"].update({"inn": customer.get("company").get("inn")})

    if customer.get("company").get("kpp"):
        result["customer"].update({"kpp": customer.get("company").get("kpp")})

    # contactPerson
    if item_contact_name[0]:
        result["contactPerson"].update({"lastName": item_contact_name[0]})

    if item_contact_name[1]:
        result["contactPerson"].update({"firstName": item_contact_name[1]})

    if detail.get("contactEmail"):
        result["contactPerson"].update({"contactEMail": detail.get("contactEmail")})

    if detail.get("contactPhone"):
        result["contactPerson"].update({"contactPhone": detail.get("contactPhone")})

    return result


def get_auction(item, detail, item_url, customer) -> dict:
    deliveries = detail.get("deliveries")[0]

    result = {
        "fz": "ЗМО",
        "purchaseNumber": item.get("number"),
        "url": item_url,
        "title": item.get("name"),
        "purchaseType": "Котировочная сессия",
        "procedureInfo": {
            "endDate": item.get("endDate")
        },
        "lots": [{
            "price": detail.get("startCost"),
            "customerRequirements": [{
                "kladrPlaces": deliveries.get("deliveryPlace"),
                "obesp_i": detail.get("contractGuaranteeAmount")
            }],
        }],
        "ETP": {
            "name": "zakupki.mos.ru"
        },
        "attachments": [{
            "docDescription": doc.get("name"),
            "url": f"https://zakupki.mos.ru/newapi/api/FileStorage/Download?id={doc.get('id')}"
        } for doc in detail.get("files")],
        "type": 2
    }

    # customer
    if customer.get("company").get("factAddress"):
        result["customer"].update({"factAddress": customer.get("company").get("factAddress")})

    if customer.get("company").get("inn"):
        result["customer"].update({"inn": customer.get("company").get("inn")})

    if customer.get("company").get("kpp"):
        result["customer"].update({"kpp": customer.get("company").get("kpp")})

    return result


def get_tender(item, detail, item_url, customer):
    register_number = detail.get("registerNumber")

    result = {
        "fz": "ЗМО",
        "purchaseNumber": item.get("registrationNumber"),
        "url": item_url,
        "title": detail.get("name"),
        "purchaseType": "Закупка 44-ФЗ и 223-ФЗ",
        "procedureInfo": {
            "endDate": item.get("endDate")
        },
        "lots": [{
            "price": detail.get("sum"),
            "customerRequirements": [{
                "kladrPlaces": [{
                    "deliveryPlace": detail.get("lot")[0].get("lotSpecification")[0].get("deliveryPlace"),
                }]
            }],
        }],
        "ETP": {
            "name": "zakupki.mos.ru"
        },
        "attachments": [{
            "docDescription": "Сведения о процедуре закупки в Единой информационной системе",
            "url": f"https://zakupki.gov.ru/epz/order/notice/ok504/view/common-info.html?regNumber={register_number}"
        },
        {
            "docDescription": "Документация в ЕИС",
            "url": f"https://zakupki.gov.ru/epz/order/notice/ok504/view/documents.html?regNumber={register_number}"
        },
        {
            "docDescription": "Протоколы в ЕИС",
            "url": f"https://zakupki.gov.ru/epz/order/notice/ok504/view/documents.html?regNumber={register_number}"
        }],
        "type": 2
    }

    # customer
    if customer.get("company").get("factAddress"):
        result["customer"].update({"factAddress": customer.get("company").get("factAddress")})

    if customer.get("company").get("inn"):
        result["customer"].update({"inn": customer.get("company").get("inn")})

    if customer.get("company").get("kpp"):
        result["customer"].update({"kpp": customer.get("company").get("kpp")})

    return result


if __name__ == "__main__":
    time_start = datetime.datetime.now()
    print(f"[SCRIPT START] - {time_start.strftime('%d.%m.%Y, %H:%M:%S')}")

    logger.info(f"Script start")

    if not os.path.exists("./data"):
        os.mkdir("./data")
        print(f"[CREATE DIR] - ./data")

    mos_url = "https://old.zakupki.mos.ru/api/Cssp/Purchase/Query?queryDto=%7B%22filter%22%3A%7B%22auctionSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B19000002%5D%7D%2C%22needSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B20000002%5D%7D%2C%22tenderSpecificFilter%22%3A%7B%22stateIdIn%22%3A%5B5%5D%7D%7D%2C%22order%22%3A%5B%7B%22field%22%3A%22relevance%22%2C%22desc%22%3Atrue%7D%5D%2C%22withCount%22%3Atrue%2C%22skip%22%3A0%7D"
    data_filepath = "./data/data.json"
    # is_created_data_file = False
    # if not os.path.exists(data_filepath):
    #     is_created_data_file = create_data_file(mos_url, data_filepath)
    # else:
    #     print(f"[ERROR] - {data_filepath} already exist")
    #     logger.error(f"{data_filepath} already exist, new orders doesn`t loaded")
    #     # os.remove(f"{data_filepath}")
    #     # logger.info(f"{data_filepath} deleted")

    orders = {}
    result = {
        "new_orders": -1,
        "errors": -1
    }
    is_created_data_file = True
    if is_created_data_file:
        result = get_orders(data_filepath)
        os.remove(f"{data_filepath}")

    time_finish = datetime.datetime.now()
    print(f"[SCRIPT FINISH] - {time_finish.strftime('%d.%m.%Y, %H:%M:%S')}")

    logger.info(f"Script finish. "
                f"{result.get('new_orders')} new orders sent. "
                f"{result.get('errors')} orders with errors.")
