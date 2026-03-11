from __future__ import print_function
import datetime
import time
import requests
from datetime import timedelta
from pytz import timezone
import pandas as pd
import gzip
from io import BytesIO
import upstox_client
from upstox_client.rest import ApiException
import ast
import json
import pytz

access_token = open("upstox_access_token.txt", "r").read()
gzipped_file_url = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
)
response = requests.get(gzipped_file_url)
gzipped_content = BytesIO(response.content)

with gzip.open(gzipped_content, "rb") as f:
    df2 = pd.read_csv(f)
    df2.to_csv("111.csv")


def getNiftyExpiryDate():
    nifty_expiry = {
        #    datetime.datetime(2025, 1, 2).date(): '25102',
        #    datetime.datetime(2025, 1, 9).date(): '25109',
        #    datetime.datetime(2025, 1, 16).date(): '25116',
        #    datetime.datetime(2025, 1, 23).date(): '25123',
        #    datetime.datetime(2025, 1, 30).date(): '25JAN',
        #    datetime.datetime(2025, 2, 6).date(): '25206',
        #    datetime.datetime(2025, 2, 13).date(): '25213',
        #    datetime.datetime(2025, 2, 20).date(): '25220',
        #    datetime.datetime(2025, 2, 27).date(): '25FEB',
        #    datetime.datetime(2025, 3, 6).date(): '25306',
        #    datetime.datetime(2025, 3, 13).date(): '25313',
        #    datetime.datetime(2025, 3, 20).date(): '25320',
        #    datetime.datetime(2025, 3, 27).date(): '25MAR',
        #    datetime.datetime(2025, 4, 3).date(): '25403',
        #    datetime.datetime(2025, 4, 10).date(): '25409',
        #    datetime.datetime(2025, 4, 17).date(): '25417',
        #    datetime.datetime(2025, 4, 24).date(): '25APR',
        #    datetime.datetime(2025, 4, 30).date(): '25430',
        datetime.datetime(2025, 5, 8).date(): "25508",
        datetime.datetime(2025, 5, 15).date(): "25515",
        datetime.datetime(2025, 5, 22).date(): "25522",
        datetime.datetime(2025, 5, 29).date(): "25MAY",
        datetime.datetime(2025, 6, 5).date(): "25605",
        datetime.datetime(2025, 6, 12).date(): "25612",
        datetime.datetime(2025, 6, 19).date(): "25619",
        datetime.datetime(2025, 6, 26).date(): "25JUN",
        datetime.datetime(2025, 7, 3).date(): "25703",
        datetime.datetime(2025, 7, 10).date(): "25710",
        datetime.datetime(2025, 7, 17).date(): "25717",
        datetime.datetime(2025, 7, 24).date(): "25724",
        datetime.datetime(2025, 7, 31).date(): "25JUL",
        datetime.datetime(2025, 8, 7).date(): "25807",
        datetime.datetime(2025, 8, 14).date(): "25814",
        datetime.datetime(2025, 8, 21).date(): "25821",
        datetime.datetime(2025, 8, 28).date(): "25AUG",
        datetime.datetime(2025, 9, 4).date(): "25904",
        datetime.datetime(2025, 9, 11).date(): "25911",
        datetime.datetime(2025, 9, 18).date(): "25918",
        datetime.datetime(2025, 9, 25).date(): "25SEP",
        datetime.datetime(2025, 10, 1).date(): "25O01",
        datetime.datetime(2025, 10, 9).date(): "25O09",
        datetime.datetime(2025, 10, 16).date(): "25O16",
        datetime.datetime(2025, 10, 23).date(): "25O23",
        datetime.datetime(2025, 10, 30).date(): "25OCT",
        datetime.datetime(2025, 11, 6).date(): "25N06",
        datetime.datetime(2025, 11, 13).date(): "25N13",
        datetime.datetime(2025, 11, 20).date(): "25N20",
        datetime.datetime(2025, 11, 27).date(): "25NOV",
        datetime.datetime(2025, 12, 4).date(): "25D04",
        datetime.datetime(2025, 12, 11).date(): "25D11",
        datetime.datetime(2025, 12, 18).date(): "25D18",
        datetime.datetime(2025, 12, 25).date(): "25DEC",
    }

    today = datetime.datetime.now().date()

    for date_key, value in nifty_expiry.items():
        if today <= date_key:
            print(value)
            return value


def getNiftyNextExpiryDate():
    nifty_expiry = {
        #    datetime.datetime(2025, 1, 2).date(): '25109',
        #    datetime.datetime(2025, 1, 9).date(): '25116',
        #    datetime.datetime(2025, 1, 16).date(): '25123',
        #    datetime.datetime(2025, 1, 23).date(): '25JAN',
        #    datetime.datetime(2025, 1, 30).date(): '25206',
        #    datetime.datetime(2025, 2, 6).date(): '25213',
        #    datetime.datetime(2025, 2, 13).date(): '25220',
        #    datetime.datetime(2025, 2, 20).date(): '25FEB',
        #    datetime.datetime(2025, 2, 27).date(): '25306',
        #    datetime.datetime(2025, 3, 6).date(): '25313',
        #    datetime.datetime(2025, 3, 13).date(): '25320',
        #    datetime.datetime(2025, 3, 20).date(): '25MAR',
        #    datetime.datetime(2025, 3, 27).date(): '25403',
        #    datetime.datetime(2025, 4, 3).date(): '25410',
        #    datetime.datetime(2025, 4, 10).date(): '25417',
        #    datetime.datetime(2025, 4, 17).date(): '25APR',
        #    datetime.datetime(2025, 4, 24).date(): '25430',
        #    datetime.datetime(2025, 4, 30).date(): '25508',
        datetime.datetime(2025, 5, 8).date(): "25515",
        datetime.datetime(2025, 5, 15).date(): "25522",
        datetime.datetime(2025, 5, 22).date(): "25MAY",
        datetime.datetime(2025, 5, 29).date(): "25605",
        datetime.datetime(2025, 6, 5).date(): "25612",
        datetime.datetime(2025, 6, 12).date(): "25619",
        datetime.datetime(2025, 6, 19).date(): "25JUN",
        datetime.datetime(2025, 6, 26).date(): "25703",
        datetime.datetime(2025, 7, 3).date(): "25710",
        datetime.datetime(2025, 7, 10).date(): "25717",
        datetime.datetime(2025, 7, 17).date(): "25724",
        datetime.datetime(2025, 7, 24).date(): "25JUL",
        datetime.datetime(2025, 7, 31).date(): "25807",
        datetime.datetime(2025, 8, 7).date(): "25814",
        datetime.datetime(2025, 8, 14).date(): "25821",
        datetime.datetime(2025, 8, 21).date(): "25AUG",
        datetime.datetime(2025, 8, 28).date(): "25904",
        datetime.datetime(2025, 9, 4).date(): "25911",
        datetime.datetime(2025, 9, 11).date(): "25918",
        datetime.datetime(2025, 9, 18).date(): "25SEP",
        datetime.datetime(2025, 9, 25).date(): "25O01",
        datetime.datetime(2025, 10, 1).date(): "25O09",
        datetime.datetime(2025, 10, 9).date(): "25O16",
        datetime.datetime(2025, 10, 16).date(): "25O23",
        datetime.datetime(2025, 10, 23).date(): "25OCT",
        datetime.datetime(2025, 10, 30).date(): "25N06",
        datetime.datetime(2025, 11, 6).date(): "25N13",
        datetime.datetime(2025, 11, 13).date(): "25N20",
        datetime.datetime(2025, 11, 20).date(): "25NOV",
        datetime.datetime(2025, 11, 27).date(): "25D04",
        datetime.datetime(2025, 12, 4).date(): "25D11",
        datetime.datetime(2025, 12, 11).date(): "25D18",
        datetime.datetime(2025, 12, 18).date(): "25DEC",
        datetime.datetime(2025, 12, 25).date(): "25DEC",
    }

    today = datetime.datetime.now().date()

    for date_key, value in nifty_expiry.items():
        if today <= date_key:
            print(value)
            return value


def getBankNiftyExpiryDate():
    return getStockExpiryDate()


def getFinNiftyExpiryDate():
    return getStockExpiryDate()


def getStockExpiryDate():
    stock_expiry = {
        #    datetime.datetime(2025, 1, 30).date(): "25JAN",
        #    datetime.datetime(2025, 2, 27).date(): "25FEB",
        #    datetime.datetime(2025, 3, 27).date(): "25MAR",
        datetime.datetime(2025, 4, 24).date(): "25APR",
        datetime.datetime(2025, 5, 26).date(): "25MAY",
        datetime.datetime(2025, 6, 30).date(): "25JUN",
        datetime.datetime(2025, 7, 28).date(): "25JUL",
        datetime.datetime(2025, 8, 25).date(): "25AUG",
        datetime.datetime(2025, 9, 29).date(): "25SEP",
        datetime.datetime(2025, 10, 27).date(): "25OCT",
        datetime.datetime(2025, 11, 24).date(): "25NOV",
        datetime.datetime(2025, 12, 29).date(): "25DEC",
    }

    today = datetime.datetime.now().date()

    for date_key, value in stock_expiry.items():
        if today <= date_key:
            return value


def getExpiryFormat(year, month, day, monthly):
    if monthly == 0:
        day1 = day
        if month == "JAN":
            month1 = 1
        elif month == "FEB":
            month1 = 2
        elif month == "MAR":
            month1 = 3
        elif month == "APR":
            month1 = 4
        elif month == "MAY":
            month1 = 5
        elif month == "JUN":
            month1 = 6
        elif month == "JUL":
            month1 = 7
        elif month == "AUG":
            month1 = 8
        elif month == "SEP":
            month1 = 9
        elif month == "OCT":
            month1 = "O"
        elif month == "NOV":
            month1 = "N"
        elif month == "DEC":
            month1 = "D"
    elif monthly == 1:
        day1 = ""
        month1 = month

    return str(year) + str(month1) + str(day1)


def getIndexSpot(stock):
    if stock == "BANKNIFTY":
        name = "Nifty Bank"
    elif stock == "NIFTY":
        name = "Nifty 50"
    elif stock == "FINNIFTY":
        name = "Nifty Fin Service"

    return name


def getOptionFormat(stock, intExpiry, strike, ce_pe):
    return str(stock) + str(intExpiry) + str(strike) + str(ce_pe)


def getLTP(instrument):
    print(instrument)
    token = df2[df2["tradingsymbol"] == instrument]["instrument_key"]
    token2 = df2[df2["name"] == instrument]["instrument_key"]

    if not token.empty:
        instrument_key = token.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]
    print(instrument_key)
    url = "http://localhost:4000/ltp?instrument=" + instrument_key

    try:
        resp = requests.get(url)
        resp2 = resp.json()
        resp3 = resp2["ltp"]
    except Exception as e:
        print(e)
    data = resp3
    return data


def getQuotes(instrument):
    token = df2[df2["tradingsymbol"] == instrument]["instrument_key"]
    token2 = df2[df2["name"] == instrument]["instrument_key"]

    if not token.empty:
        instrument_key = token.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]
    try:
        with open("upstox_data.json", "r") as f:
            result = json.load(f)
        return float(result.get(instrument_key, None))
    except (FileNotFoundError, ValueError, TypeError):
        return -1


def newgetQuotes(instrument):
    token = df2[df2["tradingsymbol"] == instrument]["instrument_key"]
    token2 = df2[df2["name"] == instrument]["instrument_key"]

    if not token.empty:
        instrument_key = token.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]
    else:
        return None, None

    try:
        with open("upstox_data.json", "r") as f:
            result = json.load(f)
        entry = result.get(instrument_key)
        if entry is None:
            return None, None
        # New format: { "ltp": 123.4, "prev_close": 120.0 }
        ltp = float(entry["ltp"])
        prev_close = float(entry["prev_close"])
        return ltp, prev_close
    except (FileNotFoundError, ValueError, TypeError, KeyError):
        return None, None


def manualLTP(symbol):
    configuration = upstox_client.Configuration()
    api_version = "2.0"
    configuration.access_token = access_token

    api_instance = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))

    instrument_key = None
    ex = None

    token = df2[df2["tradingsymbol"] == symbol]["instrument_key"]
    token2 = df2[df2["name"] == symbol]["instrument_key"]

    ex1 = df2[df2["tradingsymbol"] == symbol]["exchange"]
    ex2 = df2[df2["name"] == symbol]["exchange"]

    if not token.empty:
        instrument_key = token.values[0]
        ex = ex1.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]
        ex = ex2.values[0]

    if instrument_key is None or ex is None:
        print(f"[ERROR] Instrument not found in master for symbol: {symbol}")
        return None

    try:
        api_response = api_instance.ltp(instrument_key, api_version)
        my_dict = ast.literal_eval(str(api_response))

        symb = f"{ex}:{symbol}"
        last_price = my_dict["data"][symb]["last_price"]
        return float(last_price)

    except Exception as e:
        print(f"[ERROR] LTP fetch failed for {symbol}: {e}")
        return None


# def newmanualLTP(symbol):
#     configuration = upstox_client.Configuration()
#     configuration.access_token = access_token
#     api_version = "2.0"

#     # Look up instrument_key and exchange from df2
#     token = df2[df2["tradingsymbol"] == symbol]["instrument_key"]
#     token2 = df2[df2["name"] == symbol]["instrument_key"]
#     ex1 = df2[df2["tradingsymbol"] == symbol]["exchange"]
#     ex2 = df2[df2["name"] == symbol]["exchange"]

#     instrument_key = None
#     ex = None

#     if not token.empty:
#         instrument_key = token.values[0]
#         ex = ex1.values[0]
#     elif not token2.empty:
#         instrument_key = token2.values[0]
#         ex = ex2.values[0]

#     if instrument_key is None or ex is None:
#         print(f"[ERROR] Instrument not found in master for symbol: {symbol}")
#         return None, None

#     try:
#         # Use full quotes endpoint to get both ltp and prev_close (ohlc.close)
#         url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}"
#         headers = {
#             "Accept": "application/json",
#             "Authorization": "Bearer " + access_token,
#         }
#         response = requests.get(url, headers=headers)
#         data = response.json()

#         # Upstox key format is "EX:SYMBOL" e.g. "NSE_EQ:RELIANCE"
#         symb = f"{ex}:{symbol}"
#         entry = data["data"].get(symb)

#         if entry is None:
#             # fallback: just grab first item if key format differs
#             entry = next(iter(data["data"].values()), None)

#         if entry is None:
#             print(f"[ERROR] No data returned from Upstox for {symbol}")
#             return None, None

#         ltp = float(entry["last_price"])
#         prev_close = float(entry.get("ohlc", {}).get("close", ltp))
#         return ltp, prev_close

#     except Exception as e:
#         print(f"[ERROR] manualLTP API call failed for {symbol}: {e}")
#         return None, None


def manualLTP1(symbol):
    configuration = upstox_client.Configuration()
    configuration.access_token = access_token
    api_version = "2.0"

    # Look up instrument_key and exchange from df2
    token = df2[df2["tradingsymbol"] == symbol]["instrument_key"]
    token2 = df2[df2["name"] == symbol]["instrument_key"]
    ex1 = df2[df2["tradingsymbol"] == symbol]["exchange"]
    ex2 = df2[df2["name"] == symbol]["exchange"]

    instrument_key = None
    ex = None

    if not token.empty:
        instrument_key = token.values[0]
        ex = ex1.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]
        ex = ex2.values[0]

    if instrument_key is None or ex is None:
        print(f"[ERROR] Instrument not found in master for symbol: {symbol}")
        return None, None

    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer " + access_token,
    }
    try:
        url = f"https://api.upstox.com/v2/market-quote/quotes?instrument_key={instrument_key}"
        response = requests.get(url, headers=headers)
        data = response.json()

        if data.get("status") == "success" and data.get("data"):
            symb = f"{ex}:{symbol}"
            entry = data["data"].get(symb)

            # Fallback: grab first item if key format differs slightly
            if entry is None:
                entry = next(iter(data["data"].values()), None)

            if entry and float(entry.get("last_price", 0)) > 0:
                ltp = float(entry["last_price"])
                prev_close = float(entry.get("ohlc", {}).get("close", ltp))
                print(f"[LTP] {symbol} via quotes: {ltp}, prev_close: {prev_close}")
                return ltp, prev_close

    except Exception as e:
        print(f"[WARN] quotes endpoint failed for {symbol}: {e}")


# def manualLTP(symbol):
#     configuration = upstox_client.Configuration()
#     configuration.access_token = access_token
#     api_version = "2.0"

#     api_instance = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))

#     token_row = df2[df2["tradingsymbol"] == symbol]
#     name_row = df2[df2["name"] == symbol]

#     if not token_row.empty:
#         instrument_key = token_row["instrument_key"].values[0]
#         exchange = token_row["exchange"].values[0]
#     elif not name_row.empty:
#         instrument_key = name_row["instrument_key"].values[0]
#         exchange = name_row["exchange"].values[0]
#     else:
#         print(f"[ERROR] Instrument not found: {symbol}")
#         return None

#     try:
#         response = api_instance.ltp(instrument_key, api_version)
#         data = response.data
#         key = f"{exchange}:{symbol}"
#         return float(data[key]["last_price"])
#     except Exception as e:
#         print("[LTP ERROR]", e)
#         return None


def placeOrder(
    inst,
    t_type,
    qty,
    order_type,
    price,
    variety,
    papertrading=0,
    productType="intraday_eq",
):
    # Configure OAuth2 access token for authorization: OAUTH2
    # https://upstox.com/developer/api-documentation/#tag/Order/operation/placeOrder
    configuration = upstox_client.Configuration()
    api_version = "2.0"
    # Login and authorization
    configuration.access_token = access_token

    print(df2)

    if not df2[df2["tradingsymbol"] == inst].empty:
        instrument_key = df2[df2["tradingsymbol"] == inst]["instrument_key"].values[0]
    # If no data for 'tradingsymbol', check for 'name'
    else:
        print(inst)
        instrument_key = df2[df2["name"] == inst]["instrument_key"].values[0]
    # instrument_key = df2[df2['tradingsymbol'] == inst]['instrument_key'].values[0]

    # papertrading = 1 #if this is 1, then real trades will be placed
    dt = datetime.datetime.now()

    if order_type == "MARKET":
        price = 0

    if variety == "regular":
        variety = False
    else:
        variety = True

    if productType == "intraday_eq" or productType == "intraday_fno":
        productType = "I"
    else:
        productType = "D"

    try:
        if papertrading == 1:
            order_details = {
                "quantity": qty,
                "product": productType,
                "validity": "DAY",
                "price": price,
                "tag": "string",
                "instrument_token": instrument_key,
                "order_type": order_type,
                "transaction_type": t_type,
                "disclosed_quantity": 0,
                "trigger_price": price,
                "is_amo": variety,
            }

            api_instance = upstox_client.OrderApi(
                upstox_client.ApiClient(configuration)
            )
            api_response = api_instance.place_order(order_details, api_version)
            print(
                dt.hour,
                ":",
                dt.minute,
                ":",
                dt.second,
                " => ",
                inst,
                api_response.data.order_id,
            )
            return api_response.data.order_id
        else:
            return 0

    except Exception as e:
        print(
            dt.hour,
            ":",
            dt.minute,
            ":",
            dt.second,
            " => ",
            inst,
            "Failed : {} ".format(e),
        )


def getHistorical(ticker, interval, duration):
    # Configure OAuth2 access token for authorization: OAUTH2
    # https://upstox.com/developer/api-documentation/#tag/Order/operation/placeOrder
    configuration = upstox_client.Configuration()
    api_version = "2.0"
    # Login and authorization
    configuration.access_token = access_token

    token = df2[df2["tradingsymbol"] == ticker]["instrument_key"]
    token2 = df2[df2["name"] == ticker]["instrument_key"]

    if not token.empty:
        instrument_key = token.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]

    interval_str = "1minute"
    to_date = datetime.datetime.now().strftime("%Y-%m-%d")
    duration1 = timedelta(days=int(duration))
    from_date = datetime.datetime.now() - duration1
    from_date_str = from_date.strftime("%Y-%m-%d")
    print(from_date_str)

    # getting historical data
    try:
        # Historical candle data
        api_instance = upstox_client.HistoryApi()
        api_response = api_instance.get_historical_candle_data1(
            instrument_key, interval_str, to_date, from_date_str, api_version
        )
        # print(api_response)
        candles_data = api_response.data.candles

        # Define column names
        column_names = [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "openinterest",
        ]

        # Create a DataFrame with the specified column names
        df = pd.DataFrame(candles_data, columns=column_names)
        df["datetime2"] = df["date"].copy()
        df.set_index("date", inplace=True)
        # print(df)

    except ApiException as e:
        print(
            "Exception when calling HistoryApi->get_historical_candle_data1: %s\n" % e
        )

    # getting intraday data
    try:
        # Intra day candle data
        api_response = api_instance.get_intra_day_candle_data(
            instrument_key, interval_str, api_version
        )
        # pprint(api_response)
        candles_data = api_response.data.candles

        # Create a DataFrame with the specified column names
        df3 = pd.DataFrame(candles_data, columns=column_names)
        df3["datetime2"] = df3["date"].copy()
        df3.set_index("date", inplace=True)
        # print(df3)

    except ApiException as e:
        print("Exception when calling HistoryApi->get_intra_day_candle_data: %s\n" % e)

    merged_df = pd.concat([df3, df], ignore_index=False)
    # Convert the index to datetime explicitly
    merged_df.index = pd.to_datetime(merged_df.index)
    sorted_df = merged_df.sort_index(ascending=True)

    # Function to calculate dynamic origin based on the date (9:15 AM IST)
    def get_daily_origin(date):
        return pytz.timezone("Asia/Kolkata").localize(
            datetime.datetime(date.year, date.month, date.day, 9, 15)
        )

    # Create the dynamic resampling logic for each day
    resampled_df_list = []

    for date, group in sorted_df.groupby(sorted_df.index.date):
        # Get the dynamic origin for the current day
        origin = get_daily_origin(date)

        # Resample each day's group based on the 'interval' you want (e.g., 30min, 35min, etc.)
        resampled_day = group.resample(f"{interval}T", label="left", origin=origin).agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
                "datetime2": "first",
            }
        )

        # Append the resampled data to the list
        resampled_df_list.append(resampled_day)

    # Combine the resampled data from all days
    final_resampled_df = pd.concat(resampled_df_list)

    # Drop rows with missing 'open' values
    final_resampled_df = final_resampled_df.dropna(subset=["open"])

    return final_resampled_df


def getHistorical_Feb2025(ticker, interval, duration):
    # Configure OAuth2 access token for authorization: OAUTH2
    # https://upstox.com/developer/api-documentation/#tag/Order/operation/placeOrder
    configuration = upstox_client.Configuration()
    api_version = "2.0"
    # Login and authorization
    configuration.access_token = access_token

    token = df2[df2["tradingsymbol"] == ticker]["instrument_key"]
    token2 = df2[df2["name"] == ticker]["instrument_key"]

    if not token.empty:
        instrument_key = token.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]

    if interval == 1:
        interval_str = "1minute"
    elif interval == 30:
        interval_str = "30minute"

    interval_str = "1minute"
    to_date = datetime.datetime.now().strftime("%Y-%m-%d")
    duration1 = timedelta(days=int(duration))
    from_date = datetime.datetime.now() - duration1
    from_date_str = from_date.strftime("%Y-%m-%d")
    print(from_date_str)

    # getting historical data
    try:
        # Historical candle data
        api_instance = upstox_client.HistoryApi()
        api_response = api_instance.get_historical_candle_data1(
            instrument_key, interval_str, to_date, from_date_str, api_version
        )
        # print(api_response)
        candles_data = api_response.data.candles

        # Define column names
        column_names = [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "openinterest",
        ]

        # Create a DataFrame with the specified column names
        df = pd.DataFrame(candles_data, columns=column_names)
        df["datetime2"] = df["date"].copy()
        df.set_index("date", inplace=True)
        # print(df)

    except ApiException as e:
        print(
            "Exception when calling HistoryApi->get_historical_candle_data1: %s\n" % e
        )

    # getting intraday data
    try:
        # Intra day candle data
        api_response = api_instance.get_intra_day_candle_data(
            instrument_key, interval_str, api_version
        )
        # pprint(api_response)
        candles_data = api_response.data.candles

        # Create a DataFrame with the specified column names
        df3 = pd.DataFrame(candles_data, columns=column_names)
        df3["datetime2"] = df3["date"].copy()
        df3.set_index("date", inplace=True)
        # print(df3)
    except ApiException as e:
        print("Exception when calling HistoryApi->get_intra_day_candle_data: %s\n" % e)

    merged_df = pd.concat([df3, df], ignore_index=False)
    # Convert the index to datetime explicitly
    merged_df.index = pd.to_datetime(merged_df.index)
    sorted_df = merged_df.sort_index(ascending=True)
    # finaltimeframe = str(interval)  + "min"
    if interval < 375:
        finaltimeframe = str(interval) + "min"
    elif interval == 375:
        finaltimeframe = "D"

    # Resample to a specific time frame, for example, 30 minutes
    resampled_df = sorted_df.resample(finaltimeframe).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "datetime2": "first",
        }
    )

    # If you want to fill any missing values with a specific method, you can use fillna
    # resampled_df = resampled_df.fillna(method='ffill')  # Forward fill

    # print(resampled_df)
    resampled_df = resampled_df.dropna(subset=["open"])

    return resampled_df


def getHistorical_old(ticker, interval, duration):
    # Configure OAuth2 access token for authorization: OAUTH2
    # https://upstox.com/developer/api-documentation/#tag/Order/operation/placeOrder
    configuration = upstox_client.Configuration()
    api_version = "2.0"
    # Login and authorization
    configuration.access_token = access_token

    token = df2[df2["tradingsymbol"] == ticker]["instrument_key"]
    token2 = df2[df2["name"] == ticker]["instrument_key"]

    if not token.empty:
        instrument_key = token.values[0]
    elif not token2.empty:
        instrument_key = token2.values[0]

    if interval == 1:
        interval_str = "1minute"
    elif interval == 30:
        interval_str = "30minute"

    to_date = datetime.datetime.now().strftime("%Y-%m-%d")
    duration1 = timedelta(days=int(duration))
    from_date = datetime.datetime.now() - duration1
    from_date_str = from_date.strftime("%Y-%m-%d")
    print(from_date_str)

    # getting historical data
    try:
        # Historical candle data
        api_instance = upstox_client.HistoryApi()
        api_response = api_instance.get_historical_candle_data1(
            instrument_key, interval_str, to_date, from_date_str, api_version
        )
        # print(api_response)
        candles_data = api_response.data.candles

        # Define column names
        column_names = ["date", "open", "high", "low", "close", "volume", "oi"]

        # Create a DataFrame with the specified column names
        df = pd.DataFrame(candles_data, columns=column_names)
        df.set_index("date", inplace=True)
        # print(df)

    except ApiException as e:
        print(
            "Exception when calling HistoryApi->get_historical_candle_data1: %s\n" % e
        )

    # getting intraday data
    try:
        # Intra day candle data
        api_response = api_instance.get_intra_day_candle_data(
            instrument_key, interval_str, api_version
        )
        # pprint(api_response)
        candles_data = api_response.data.candles

        # Create a DataFrame with the specified column names
        df3 = pd.DataFrame(candles_data, columns=column_names)
        df3.set_index("date", inplace=True)
        # print(df3)
    except ApiException as e:
        print("Exception when calling HistoryApi->get_intra_day_candle_data: %s\n" % e)

    merged_df = pd.concat([df3, df], ignore_index=False)
    sorted_df = merged_df.sort_index(ascending=True)
    # print(merged_df)
    return sorted_df
