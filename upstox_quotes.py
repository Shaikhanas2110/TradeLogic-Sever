# Import necessary modules
import asyncio
import json
import ssl
import upstox_client
import websockets
from google.protobuf.json_format import MessageToDict
from zipfile import ZipFile
import requests
import os
from csv import DictReader
import pandas as pd
import csv
from flask import Flask, request
import threading
import ast
import re
import helper_upstox as helper
from upstox_client.feeder.proto.MarketDataFeedV3_pb2 import FeedResponse
import time

access_token = open("upstox_access_token.txt", "r").read()
print(access_token)
instr_dir = "/csvs"  #'./csvs" for mac
instr_token_dict = {}
instr_ltp_dict = {}

nf_expiry = helper.getNiftyExpiryDate()
bnf_expiry = helper.getBankNiftyExpiryDate()
fin_expiry = helper.getFinNiftyExpiryDate()

# Put all options in the beginning
symbolList1 = [
    "NSE_INDEX|Nifty 50",
    "NSE_INDEX|Nifty Bank",
    "NSE_INDEX|NIFTY MID SELECT",
    "NSE_INDEX|Nifty Fin Service",
    "NSE_EQ|RELIANCE",
]

app = Flask(__name__)


def get_ltp(api_version, configuration, instrument_key):
    api_instance = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))
    api_response = api_instance.ltp(instrument_key, api_version)

    parsed = ast.literal_eval(str(api_response))

    return parsed


def get_market_data_feed_authorize(api_version, configuration):
    """Get authorization for market data feed."""
    api_instance = upstox_client.WebsocketApi(upstox_client.ApiClient(configuration))
    api_response = api_instance.get_market_data_feed_authorize(api_version)
    return api_response


def decode_protobuf(buffer):
    """Decode protobuf message."""
    feed_response = FeedResponse()
    feed_response.ParseFromString(buffer)
    return feed_response


def download_csv(url):
    r = requests.get(url, allow_redirects=True)

    if not os.path.isdir(instr_dir):
        os.mkdir(instr_dir)

    filepath = f"{instr_dir}/{url.split('/')[-1]}"

    open(filepath, "wb").write(r.content)


def csv_process():
    global instr_token_dict

    download_csv(
        "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    )

    # open file in read mode
    df = pd.read_csv(
        f"{instr_dir}/complete.csv.gz", delimiter=",", quoting=csv.QUOTE_NONE
    )
    df.to_csv(f"{instr_dir}/temp.csv")

    with open(f"{instr_dir}/temp.csv", newline="") as csvfile:
        row_csv = csv.DictReader(csvfile, delimiter=",")
        for row in row_csv:
            instr_token_dict[row['"tradingsymbol"'].replace('"', "")] = row[
                '"instrument_key"'
            ].replace('"', "")

    with open("instr_token_dict.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Key", "Value"])  # Optional header
        for key, value in instr_token_dict.items():
            writer.writerow([key, value])


# def fetch_market_data():
#     global symbolList1

#     """Fetch market data using WebSocket and print it."""
#     csv_process()

#     # Configure OAuth2 access token for authorization
#     configuration = upstox_client.Configuration()

#     api_version = "2.0"
#     configuration.access_token = access_token

#     nf_intExpiry = nf_expiry
#     bnf_intExpiry = bnf_expiry
#     fin_intExpiry = fin_expiry
#     strikeList = []
#     symbolList = []

#     # NIFTY
#     ltp = get_ltp(api_version, configuration, "NSE_INDEX|Nifty 50")
#     a = float(ltp["data"]["NSE_INDEX:Nifty 50"]["last_price"])

#     for i in range(-5, 5):
#         strike = (int(a / 100) + i) * 100
#         strikeList.append(strike)
#         strikeList.append(strike + 50)

#     # Add CE
#     for strike in strikeList:
#         ltp_option = "NIFTY" + str(nf_intExpiry) + str(strike) + "CE"
#         symbolList.append(ltp_option)

#     # Add PE
#     for strike in strikeList:
#         ltp_option = "NIFTY" + str(nf_intExpiry) + str(strike) + "PE"
#         symbolList.append(ltp_option)

#     strikeList = []

#     # BANKNIFTY
#     ltp = get_ltp(api_version, configuration, "NSE_INDEX|Nifty Bank")
#     a = float(ltp["data"]["NSE_INDEX:Nifty Bank"]["last_price"])

#     for i in range(-5, 5):
#         strike = (int(a / 100) + i) * 100
#         strikeList.append(strike)

#     # Add CE
#     for strike in strikeList:
#         ltp_option = "BANKNIFTY" + str(bnf_intExpiry) + str(strike) + "CE"
#         symbolList.append(ltp_option)

#     # Add PE
#     for strike in strikeList:
#         ltp_option = "BANKNIFTY" + str(bnf_intExpiry) + str(strike) + "PE"
#         symbolList.append(ltp_option)

#     # FINNIFTY
#     strikeList = []
#     ltp = get_ltp(api_version, configuration, "NSE_INDEX|Nifty Fin Service")
#     a = float(ltp["data"]["NSE_INDEX:Nifty Fin Service"]["last_price"])

#     for i in range(-5, 5):
#         strike = (int(a / 100) + i) * 100
#         strikeList.append(strike)
#         strikeList.append(strike + 50)

#     # Add CE
#     for strike in strikeList:
#         ltp_option = "FINNIFTY" + str(fin_intExpiry) + str(strike) + "CE"
#         symbolList.append(ltp_option)

#     # Add PE
#     for strike in strikeList:
#         ltp_option = "FINNIFTY" + str(fin_intExpiry) + str(strike) + "PE"
#         symbolList.append(ltp_option)

#     symbolList = symbolList + symbolList1
#     # symbolList = symbolList1
#     print("BELOW IS THE COMPLETE INSTRUMENT LIST")
#     print(symbolList)
#     backup_symbolList = symbolList[:]
#     newsymbolList = []
#     count = 0

#     for symbol in symbolList:
#         opt_search = re.search(r"(\d{2})(\w{3})((\d+)|(\d+\.\d+))(CE|PE)", symbol)
#         fut_search = re.search(r"(\d{2}\w{3})(FUT)", symbol)

#         if opt_search or fut_search:
#             newsymbolList.append(instr_token_dict[symbol])

#         count += 1

#     for symbol in symbolList:
#         if symbol.startswith("NSE_INDEX"):
#             newsymbolList.append(symbol)

#         count += 1

#     for symbol in symbolList:
#         if symbol.startswith("NSE_EQ"):
#             newsymbolList.append(instr_token_dict[symbol[7:]])

#         count += 1

#     print("new symbollist")
#     print(newsymbolList)
#     symbolList = newsymbolList
#     # print(instr_token_dict)
#     print("SYMBOLLIST")
#     print(symbolList)

#     symbol_dict = dict(zip(symbolList, backup_symbolList))
#     print("SYMBOL_DICT")
#     print(symbol_dict)

#     ###### INPUTS END ######

#     # Create default SSL context
#     ssl_context = ssl.create_default_context()
#     ssl_context.check_hostname = False
#     ssl_context.verify_mode = ssl.CERT_NONE

#     quotes_list = ", ".join([f"'{symbol}'" for symbol in symbolList])
#     quotes_list = quotes_list.replace(" '", "")
#     quotes_list = quotes_list.replace("'", "")
#     print("QUOTES LIST")
#     print(quotes_list)

#     # quotes_list = "NSE_FO|45444,NSE_FO|45446,NSE_FO|45450,NSE_FO|45453"
#     while True:
#         url = "https://api.upstox.com/v2/market-quote/ltp?instrument_key=" + quotes_list
#         headers = {
#             "Accept": "application/json",
#             "Authorization": "Bearer " + access_token,
#         }

#         response = requests.get(url, headers=headers)
#         all_quotes = json.loads(response.text)
#         # print(all_quotes)
#         result = {
#             item["instrument_token"]: item["last_price"]
#             for item in all_quotes["data"].values()
#         }
#         print(result)
#         # Write to a JSON file
#         with open("upstox_data.json", "w") as f:
#             json.dump(result, f)
#             print("updated json at " + time.strftime("%Y-%m-%d %H:%M:%S"))
#         time.sleep(10)


def fetch_market_data():
    global symbolList1

    csv_process()

    configuration = upstox_client.Configuration()
    api_version = "2.0"
    configuration.access_token = access_token

    nf_intExpiry = nf_expiry
    bnf_intExpiry = bnf_expiry
    fin_intExpiry = fin_expiry
    strikeList = []
    symbolList = []

    # NIFTY
    ltp = get_ltp(api_version, configuration, "NSE_INDEX|Nifty 50")
    a = float(ltp["data"]["NSE_INDEX:Nifty 50"]["last_price"])
    for i in range(-5, 5):
        strike = (int(a / 100) + i) * 100
        strikeList.append(strike)
        strikeList.append(strike + 50)
    for strike in strikeList:
        symbolList.append("NIFTY" + str(nf_intExpiry) + str(strike) + "CE")
    for strike in strikeList:
        symbolList.append("NIFTY" + str(nf_intExpiry) + str(strike) + "PE")
    strikeList = []

    # BANKNIFTY
    ltp = get_ltp(api_version, configuration, "NSE_INDEX|Nifty Bank")
    a = float(ltp["data"]["NSE_INDEX:Nifty Bank"]["last_price"])
    for i in range(-5, 5):
        strike = (int(a / 100) + i) * 100
        strikeList.append(strike)
    for strike in strikeList:
        symbolList.append("BANKNIFTY" + str(bnf_intExpiry) + str(strike) + "CE")
    for strike in strikeList:
        symbolList.append("BANKNIFTY" + str(bnf_intExpiry) + str(strike) + "PE")

    # FINNIFTY
    strikeList = []
    ltp = get_ltp(api_version, configuration, "NSE_INDEX|Nifty Fin Service")
    a = float(ltp["data"]["NSE_INDEX:Nifty Fin Service"]["last_price"])
    for i in range(-5, 5):
        strike = (int(a / 100) + i) * 100
        strikeList.append(strike)
        strikeList.append(strike + 50)
    for strike in strikeList:
        symbolList.append("FINNIFTY" + str(fin_intExpiry) + str(strike) + "CE")
    for strike in strikeList:
        symbolList.append("FINNIFTY" + str(fin_intExpiry) + str(strike) + "PE")

    symbolList = symbolList + symbolList1
    backup_symbolList = symbolList[:]
    newsymbolList = []

    for symbol in symbolList:
        opt_search = re.search(r"(\d{2})(\w{3})((\d+)|(\d+\.\d+))(CE|PE)", symbol)
        fut_search = re.search(r"(\d{2}\w{3})(FUT)", symbol)
        if opt_search or fut_search:
            newsymbolList.append(instr_token_dict[symbol])

    for symbol in symbolList:
        if symbol.startswith("NSE_INDEX"):
            newsymbolList.append(symbol)

    for symbol in symbolList:
        if symbol.startswith("NSE_EQ"):
            newsymbolList.append(instr_token_dict[symbol[7:]])

    symbolList = newsymbolList
    symbol_dict = dict(zip(symbolList, backup_symbolList))

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    quotes_list = ",".join(symbolList)

    while True:
        # ── CHANGED: /market-quote/ltp  →  /market-quote/quotes ─────────────
        # quotes endpoint returns full OHLC so we can get prev_close
        url = (
            "https://api.upstox.com/v2/market-quote/quotes?instrument_key="
            + quotes_list
        )
        headers = {
            "Accept": "application/json",
            "Authorization": "Bearer " + access_token,
        }

        response = requests.get(url, headers=headers)
        all_quotes = json.loads(response.text)

        result = {}
        for item in all_quotes["data"].values():
            token = item["instrument_token"]
            ltp_price = float(item.get("last_price", 0.0))
            # ohlc.close is the PREVIOUS DAY's closing price on Upstox
            prev_close = float(item.get("ohlc", {}).get("close", ltp_price))
            result[token] = {
                "ltp": round(ltp_price, 2),
                "prev_close": round(prev_close, 2),
            }

        with open("upstox_data.json", "w") as f:
            json.dump(result, f)
            print("updated json at " + time.strftime("%Y-%m-%d %H:%M:%S"))

        time.sleep(10)


fetch_market_data()
