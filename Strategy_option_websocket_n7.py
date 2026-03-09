# DISCLAIMER:
# 1) This sample code is for learning purposes only.
# 2) Always be very careful when dealing with codes in which you can place orders in your account.
# 3) The actual results may or may not be similar to backtested results. The historical results do not guarantee any profits or losses in the future.
# 4) You are responsible for any losses/profits that occur in your account in case you plan to take trades in your account.
# 5) TFU and Aseem Singhal do not take any responsibility of you running these codes on your account and the corresponding profits and losses that might occur.
# 6) The running of the code properly is dependent on a lot of factors such as internet, broker, what changes you have made, etc. So it is always better to keep checking the trades as technology error can come anytime.
# 7) This is NOT a tip providing service/code.
# 8) This is NOT a software. Its a tool that works as per the inputs given by you.
# 9) Slippage is dependent on market conditions.
# 10) Option trading and automatic API trading are subject to market risks
# ABC This seems like a straddle of CE and PE both

import datetime
import time
import pandas as pd
import requests

####################__INPUT__#####################
# TIME TO FIND THE STRIKE
entryHour = 0
entryMinute = 0
entrySecond = 0
startTime = datetime.time(entryHour, entryMinute, entrySecond)

tradecounter = 0

while tradecounter < 3:
    tradecounter += 1
stock = "NIFTY"  # BANKNIFTY OR NIFTY OR FINNIFTY             # ABC SET TRADE HERE
otm = 0  # If you put -100, that means its 100 points ITM.`
SL_point = 8  # 5
target_point = 18  # 20
SL_percentage = 10
target_percentage = 10
for_every_x_point = 0  # 5
trail_by_y_point = 0  # 2
PnL = 0
premium = 85
trade_based_on = "atm"  # "premium" or "atm"
sl_based_on = "point"  # "point" or "percent"
producttpye = (
    "intraday_fno"  # "intraday_eq","positional_eq","intraday_fno","positional_fno"
)
df = pd.DataFrame(
    columns=[
        "Date",
        "CE_Entry_Price",
        "CE_Exit_Price",
        "PE_Entry_Price",
        "PE_Exit_Price",
        "PnL",
    ]
)
df["Date"] = [datetime.date.today()]
qty = 75
papertrading = 0  # If paper trading is 0, then paper trading will be done. If paper trading is 1, then live trade

# ABC df_full = pddf = pd.DataFrame(columns=['Date','CE_Entry_Price','CE_Exit_Price','PE_Entry_Price','PE_Exit_Price','PnL'])


# If you have any below brokers, then make it 1
shoonya_broker = 0
nuvama_broker = 0
icici_broker = 0
angel_broker = 0
alice_broker = 0
fyers_broker = 0
zerodha_broker = 0
upstox_broker = 1
iifl_broker = 0
dhan_broker = 0

if dhan_broker == 1:
    from dhanhq import dhanhq
    import helper_dhan as helper

    client_id = open("dhan_client_id.txt", "r").read()
    access_token = open("dhan_access_token.txt", "r").read()
    dhan = dhanhq(client_id, access_token)

if nuvama_broker == 1:
    import nuvama_login
    import helper_nuvama as helper

    api_connect = nuvama_login.api_connect

if icici_broker == 1:
    import icici_login
    import helper_icici as helper

    breeze = icici_login.breeze

if angel_broker == 1:
    import helper_angel as helper

    helper.login_trading()
    time.sleep(15)
    helper.login_historical()

if alice_broker == 1:
    import alice_login
    import helper_alice as helper

    alice = alice_login.alice


if fyers_broker == 1:
    from fyers_apiv3 import fyersModel
    import helper_fyers as helper

    app_id = open("fyers_client_id.txt", "r").read()
    access_token = open("fyers_access_token.txt", "r").read()
    fyers = fyersModel.FyersModel(token=access_token, is_async=False, client_id=app_id)

if shoonya_broker == 1:
    from NorenApi import NorenApi
    import helper_shoonya as helper

    api = NorenApi()
    api.token_setter()

if zerodha_broker == 1:
    from kiteconnect import KiteTicker
    from kiteconnect import KiteConnect
    import helper_zerodha as helper

    apiKey = open("zerodha_api_key.txt", "r").read()
    accessToken = open("zerodha_access_token.txt", "r").read()
    kc = KiteConnect(api_key=apiKey)
    kc.set_access_token(accessToken)

if upstox_broker == 1:
    import helper_upstox as helper

if iifl_broker == 1:
    from Connect import XTSConnect
    import helper_iifl as helper

    """Dealer credentials"""
    with open("iifl_api_key.txt", "r") as f:
        API_KEY = f.read()
    with open("iifl_secret_key.txt", "r") as f:
        API_SECRET = f.read()
    with open("user_id.txt", "r") as f:
        userID = f.read()
    XTS_API_BASE_URL = "https://developers.symphonyfintech.in"
    source = "WEBAPI"

    """Make XTSConnect object by passing your interactive API appKey, secretKey and source"""
    xt = XTSConnect(API_KEY, API_SECRET, source)

    """Using the xt object we created call the interactive login Request"""
    response = xt.interactive_login()
    print("Login: ", response)
##################################################


def findStrikePriceATM():
    name = helper.getIndexSpot(stock)

    if stock == "NIFTY":
        intExpiry = helper.getNiftyExpiryDate()
    else:
        intExpiry = helper.getStockExpiryDate()

    if not intExpiry:
        print("[FATAL] Expiry not resolved. Aborting entry.")
        return

    ######################################################
    # FINDING ATM
    ltp = helper.getQuotes(name)
    print(ltp)

    if stock == "BANKNIFTY":
        closest_Strike = int(round((ltp / 100), 0) * 100)
        print(closest_Strike)

    elif stock == "NIFTY" or stock == "FINNIFTY":
        closest_Strike = int(
            round((ltp / 50), 0) * 50
        )  #  22104.05 / 75 = 442*50 = 22100
        print(closest_Strike)

    print("closest", closest_Strike)
    closest_Strike_CE = closest_Strike + otm
    closest_Strike_PE = closest_Strike - otm

    atmCE = helper.getOptionFormat(stock, intExpiry, closest_Strike_CE, "CE")
    atmPE = helper.getOptionFormat(stock, intExpiry, closest_Strike_PE, "PE")

    print(stock)  # ABC Trying to get instrument name
    print(atmCE)
    print(atmPE)

    takeEntry(closest_Strike_CE, closest_Strike_PE, atmCE, atmPE)


def findStrikePricePremium():
    name = helper.getIndexSpot(stock)

    strikeList = []
    prev_diff = 10000
    closest_Strike = 10000

    if stock == "NIFTY":
        intExpiry = helper.getNiftyExpiryDate()
    else:
        intExpiry = helper.getStockExpiryDate()

    ######################################################
    # FINDING ATM
    ltp = helper.getQuotes(name)

    # premium = 85 CE
    if stock == "BANKNIFTY":
        for i in range(-8, 8):
            strike = (int(ltp / 100) + i) * 100
            strikeList.append(strike)
        print(strikeList)
    elif stock == "NIFTY" or stock == "FINNIFTY":
        for i in range(-5, 6):
            strike = (int(ltp / 100) + i) * 100  # 221 - 4 = 21700
            strikeList.append(strike)
            strikeList.append(strike + 50)
        print(strikeList)

    # FOR CE
    prev_diff = 10000
    for strike in strikeList:
        ltp_option = findManualPrice(
            helper.getOptionFormat(stock, intExpiry, strike, "CE")
        )
        print(ltp_option)
        diff = abs(ltp_option - premium)
        print("diff==>", diff)
        if diff < prev_diff:
            closest_Strike_CE = strike
            prev_diff = diff
        time.sleep(0.5)

    # FOR PE
    prev_diff = 10000
    for strike in strikeList:
        ltp_option = findManualPrice(
            helper.getOptionFormat(stock, intExpiry, strike, "PE")
        )
        diff = abs(ltp_option - premium)
        print("diff==>", diff)
        if diff < prev_diff:
            closest_Strike_PE = strike
            prev_diff = diff
        time.sleep(0.5)

    print("closest CE", closest_Strike_CE)
    print("closest PE", closest_Strike_PE)

    atmCE = helper.getOptionFormat(stock, intExpiry, closest_Strike_CE, "CE")
    atmPE = helper.getOptionFormat(stock, intExpiry, closest_Strike_PE, "PE")

    print(atmCE)
    print(atmPE)

    takeEntry(closest_Strike_CE, closest_Strike_PE, atmCE, atmPE)


def takeEntry(closest_Strike_CE, closest_Strike_PE, atmCE, atmPE):
    global PnL
    ce_entry_price = findManualPrice(atmCE)
    pe_entry_price = findManualPrice(atmPE)

    if ce_entry_price is None or pe_entry_price is None:
        print("[FATAL] LTP fetch failed. Aborting trade.")
        return

    PnL = ce_entry_price + pe_entry_price
    print("Current PnL is: ", PnL)
    df["CE_Entry_Price"] = [ce_entry_price]
    df["PE_Entry_Price"] = [pe_entry_price]

    print(" Closest_CE ATM ", closest_Strike_CE, " CE Entry Price = ", ce_entry_price)
    print(" Closest_PE ATM", closest_Strike_PE, " PE Entry Price = ", pe_entry_price)

    if sl_based_on == "point":
        ceSL = round(ce_entry_price + SL_point, 1)
        peSL = round(pe_entry_price + SL_point, 1)
        ceTarget = round(ce_entry_price - target_point, 1)
        peTarget = round(pe_entry_price - target_point, 1)
    else:
        ceSL = round(ce_entry_price * (1 + SL_percentage / 100), 1)
        peSL = round(pe_entry_price * (1 + SL_percentage / 100), 1)
        ceTarget = round(ce_entry_price * (1 - target_percentage / 100), 1)
        peTarget = round(pe_entry_price * (1 - target_percentage / 100), 1)

    print(
        "Placing Order CE Entry Price = ",
        ce_entry_price,
        "|  CE SL => ",
        ceSL,
        "| CE Target => ",
        ceTarget,
    )
    print(
        "Placing Order PE Entry Price = ",
        pe_entry_price,
        "|  PE SL => ",
        peSL,
        "| PE Target => ",
        peTarget,
    )

    # SELL AT MARKET PRICE
    oidentryCE = placeOrder1(
        atmCE,
        "SELL",
        qty,
        "MARKET",
        ce_entry_price,
        "regular",
        papertrading,
        producttpye,
    )
    oidentryPE = placeOrder1(
        atmPE,
        "SELL",
        qty,
        "MARKET",
        pe_entry_price,
        "regular",
        papertrading,
        producttpye,
    )

    print("The OID of Entry CE is: ", oidentryCE)
    print("The OID of Entry PE is: ", oidentryPE)

    exitPosition(
        atmCE,
        ceSL,
        ceTarget,
        ce_entry_price,
        atmPE,
        peSL,
        peTarget,
        pe_entry_price,
        qty,
    )


def exitPosition(
    atmCE, ceSL, ceTarget, ce_entry_price, atmPE, peSL, peTarget, pe_entry_price, qty
):
    global PnL
    traded = "No"
    originalEntryCE = ce_entry_price
    originalEntryPE = pe_entry_price
    ce_exit_done = False
    pe_exit_done = False
    while traded == "No":
        dt = datetime.datetime.now()
        try:
            ltp = helper.getQuotes(atmCE)
            ltp1 = helper.getQuotes(atmPE)

            print(
                "CE LTP: ",
                ltp,
                " CE Exit: ",
                ce_exit_done,
                "PE LTP: ",
                ltp1,
                " PE Exit: ",
                pe_exit_done,
            )

            # This is for call
            if (
                (ltp > ceSL) or (ltp < ceTarget) or (dt.hour >= 15 and dt.minute >= 10)
            ) and ce_exit_done == False:
                oidexitCE = placeOrder1(
                    atmCE,
                    "BUY",
                    qty,
                    "MARKET",
                    ceSL,
                    "regular",
                    papertrading,
                    producttpye,
                )
                PnL = PnL - ltp
                print("Current PnL is: ", PnL)
                df["CE_Exit_Price"] = [ltp]
                print(
                    "The OID of Exit CE is: ", oidexitCE, "Scrip ", stock
                )  # ABC  Added Instrument
                ce_exit_done = True

            # THis is for put
            if (
                (ltp1 > peSL)
                or (ltp1 < peTarget)
                or (dt.hour >= 15 and dt.minute >= 10)
            ) and pe_exit_done == False:
                oidexitPE = placeOrder1(
                    atmPE,
                    "BUY",
                    qty,
                    "MARKET",
                    peSL,
                    "regular",
                    papertrading,
                    producttpye,
                )
                PnL = PnL - ltp1
                print("Current PnL is: ", PnL)
                df["PE_Exit_Price"] = [ltp1]
                print(
                    "The OID of Exit PE is: ", oidexitPE, "Scrip ", stock
                )  # ABC  Added Instrument )
                pe_exit_done = True

            # trail SL CE
            if ltp < originalEntryCE - for_every_x_point:
                originalEntryCE = originalEntryCE - for_every_x_point
                ceSL = ceSL - trail_by_y_point

            # trail SL PE
            if ltp1 < originalEntryPE - for_every_x_point:
                originalEntryPE = originalEntryPE - for_every_x_point
                peSL = peSL - trail_by_y_point

            # Exit this function
            if ce_exit_done == True and pe_exit_done == True:
                print("Both Exits are done")
                break

            time.sleep(5)

        except:
            print(" ")
            time.sleep(1)


def placeOrder1(
    inst,
    t_type,
    qty,
    order_type,
    price,
    variety,
    papertrading=0,
    producttype="intraday_eq",
):
    # t_type = "BUY", "SELL"
    # order_type = "MARKET", "LIMIT"
    # variety = "regular", "amo",
    # producttype = "intraday_eq","positional_eq","intraday_fno","positional_fno"
    global api_connect
    global breeze
    global fyers
    global api
    global kc
    global alice
    global xt
    global dhan
    if papertrading == 0:
        return 0
    elif nuvama_broker == 1:
        return helper.placeOrder(
            inst,
            t_type,
            qty,
            order_type,
            price,
            variety,
            api_connect,
            papertrading,
            producttype,
        )
    elif icici_broker == 1:
        return helper.placeOrder(
            inst,
            t_type,
            qty,
            order_type,
            price,
            variety,
            breeze,
            papertrading,
            producttype,
        )
    elif alice_broker == 1:
        return helper.placeOrder(
            inst,
            t_type,
            qty,
            order_type,
            price,
            variety,
            alice,
            papertrading,
            producttype,
        )
    elif fyers_broker == 1:
        return helper.placeOrder(
            inst,
            t_type,
            qty,
            order_type,
            price,
            variety,
            fyers,
            papertrading,
            producttype,
        )
    elif shoonya_broker == 1:
        return helper.placeOrder(
            inst,
            t_type,
            qty,
            order_type,
            price,
            variety,
            api,
            papertrading,
            producttype,
        )
    elif zerodha_broker == 1:
        return helper.placeOrder(
            inst, t_type, qty, order_type, price, variety, kc, papertrading, producttype
        )
    elif iifl_broker == 1:
        return helper.placeOrder(
            inst, t_type, qty, order_type, price, variety, xt, papertrading, producttype
        )
    elif dhan_broker == 1:
        return helper.placeOrder(
            inst,
            t_type,
            qty,
            order_type,
            price,
            variety,
            dhan,
            papertrading,
            producttype,
        )
    else:
        return helper.placeOrder(
            inst, t_type, qty, order_type, price, variety, papertrading, producttype
        )


def findManualPrice(symbol):
    global api_connect
    global breeze
    global fyers
    global api
    global kc
    global alice
    global xt
    global dhan
    if zerodha_broker == 1:
        return helper.manualLTP(symbol, kc)
    elif shoonya_broker == 1:
        return helper.manualLTP(symbol, api)
    elif icici_broker == 1:
        return helper.manualLTP(symbol, breeze)
    elif fyers_broker == 1:
        return helper.manualLTP(symbol, fyers)
    elif alice_broker == 1:
        return helper.manualLTP(symbol, alice)
    elif angel_broker == 1:
        return helper.manualLTP(symbol)
    elif nuvama_broker == 1:
        return helper.getLTP(symbol)
    elif iifl_broker == 1:
        return helper.manualLTP(symbol, xt)
    elif dhan_broker == 1:
        return helper.manualLTP(symbol, dhan)
    else:
        return helper.manualLTP(symbol)


def checkTime_tofindStrike():
    x = 1
    while x == 1:
        dt = datetime.datetime.now()
        # if( dt.hour >= entryHour and dt.minute >= entryMinute and dt.second >= entrySecond ):
        if dt.time() >= startTime:
            print("Start Time Reached")
            x = 2
            if trade_based_on == "Premium":
                findStrikePricePremium()
            else:
                findStrikePriceATM()
        else:
            time.sleep(1)
            print(dt, " Waiting for Exchange Start Time to check new ATM ")


def get_ltp(symbol):
    try:
        # INDEX HANDLING
        if symbol == "NIFTY":
            return helper.getQuotes("Nifty 50")

        if symbol == "BANKNIFTY":
            return helper.getQuotes("Nifty Bank")

        # STOCK HANDLING
        return helper.manualLTP(symbol)

    except Exception as e:
        print("[LTP ERROR]", symbol, e)
        return None


def get_ltp_price(symbol):
    """Returns (ltp, prev_close) tuple for a symbol."""
    try:
        if symbol == "NIFTY":
            return helper.newgetQuotes("Nifty 50")
        if symbol == "BANKNIFTY":
            return helper.newgetQuotes("Nifty Bank")
        return helper.newmanualLTP(symbol)
    except Exception as e:
        print("[LTP ERROR]", symbol, e)
        return None, None


checkTime_tofindStrike()
df["PnL"] = [PnL]

df.to_csv("template_options.csv", mode="a", index=True, header=True)
print("Nifty Options Trade Done...")


def start_algo():
    print("[INFO] Algo started")

    try:
        checkTime_tofindStrike()  # THIS is already your main logic
    except Exception as e:
        print("[FATAL] Algo crashed:", e)
