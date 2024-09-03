import os
import requests
import json
from dotenv import load_dotenv
import pandas as pd
import time
from datetime import date

load_dotenv()
FMARKET_AUTHORIZATION = os.environ.get("FMARKET_AUTHORIZATION")

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "vi",
    "authorization": FMARKET_AUTHORIZATION,
    "content-type": "application/json",
    "origin": "https://fmarket.vn",
    "priority": "u=1, i",
    "referer": "https://fmarket.vn/",
    "sec-ch-ua": '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
}


def get_all_funds() -> pd.DataFrame:
    url = "https://api.fmarket.vn/res/products/filter"
    payload = json.dumps(
        {
            "types": ["NEW_FUND", "TRADING_FUND"],
            "issuerIds": [],
            "sortOrder": "DESC",
            "sortField": "navTo12Months",
            "page": 1,
            "pageSize": 100,
            "isIpo": False,
            "fundAssetTypes": [],
            "bondRemainPeriods": [],
            "searchField": "",
            "isBuyByReward": False,
            "thirdAppIds": [],
        }
    )
    response = requests.request("POST", url, headers=HEADERS, data=payload)

    if response.status_code == 200:
        data = response.json()["data"]["rows"]

        # list columns for storage
        columns = [
            "id",
            "name",
            "shortName",
            "code",
            "price",
            "nav",
            "lastYearNav",
            "holdingVolume",
            "firstIssueAt",
            "approveAt",
            "createAt",
            "updateAt",
            "description",
            "owner",
            "status",
            "dataFundAssetType",
            "productNavChange",
        ]
        data = [{k: v for k, v in fund.items() if k in columns} for fund in data]
        return pd.DataFrame(data)

    else:
        raise requests.exceptions.HTTPError(
            "Error when request API: {} - {}".format(
                response.status_code, {response.text}
            )
        )


def get_top_holding(fund_id) -> list:
    url = f"https://api.fmarket.vn/res/products/{fund_id}"
    response = requests.request("GET", url, headers=HEADERS, data={})

    if response.status_code == 200:
        data = response.json()["data"]
        try:
            list_hoding = (
                data["productTopHoldingList"] + data["productTopHoldingBondList"]
            )
            for product in list_hoding:
                product["fund_id"] = fund_id
            return list_hoding

        except Exception as e:
            print(e)
            return []

    else:
        raise requests.exceptions.HTTPError(
            "Error when request API: {} - {}".format(
                response.status_code, {response.text}
            )
        )


def get_asset_hoding(fund_id) -> list:
    url = f"https://api.fmarket.vn/res/products/{fund_id}"
    response = requests.request("GET", url, headers=HEADERS, data={})

    if response.status_code == 200:
        data = response.json()["data"]
        try:
            list_asset = data["productAssetHoldingList"]
            for product in list_asset:
                product["fund_id"] = fund_id

        except Exception as e:
            print(e)
            list_asset = []

        return list_asset

    else:
        raise requests.exceptions.HTTPError(
            "Error when request API: {} - {}".format(
                response.status_code, {response.text}
            )
        )


def get_industry_holding(fund_id) -> list:
    url = f"https://api.fmarket.vn/res/products/{fund_id}"
    response = requests.request("GET", url, headers=HEADERS, data={})

    if response.status_code == 200:
        data = response.json()["data"]
        list_industry = data["productIndustriesHoldingList"]
        try:
            for product in list_industry:
                product["fund_id"] = fund_id
            return list_industry

        except TypeError:
            return []

    else:
        raise requests.exceptions.HTTPError(
            "Error when request API: {} - {}".format(
                response.status_code, {response.text}
            )
        )


today = date.today().strftime("%Y%m%d")


def get_nav_history(
    fund_id: int, start_date: str = None, end_date: str = today
) -> list:
    url = "https://api.fmarket.vn/res/product/get-nav-history"
    if not start_date:
        isAllData = 1
    else:
        isAllData = 0
    payload = json.dumps(
        {
            "isAllData": isAllData,
            "productId": fund_id,
            "fromDate": start_date,
            "toDate": end_date,
        }
    )

    response = requests.request("POST", url, headers=HEADERS, data=payload)

    if response.status_code == 200:
        nav_history = response.json()["data"]
        return nav_history

    else:
        raise requests.exceptions.HTTPError(
            "Error when request API: {} - {}".format(
                response.status_code, {response.text}
            )
        )


if __name__ == "__main__":
    print(get_nav_history(68, "20240801", "20240830"))
