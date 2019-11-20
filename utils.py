from environs import Env
env = Env()
env.read_env()
api_key=env("BINANCE_APIKEY")
api_secret=env("BINANCE_APISECRET")

import math
import pandas as pd
from datetime import datetime
from binance.client import Client
client = Client(api_key, api_secret)

def get_symbols(quote_currency):
    symbols = client.get_exchange_info()['symbols']
    symbol_list = []

    symbol_blacklist = ['DASHBTC','ETHBTC','XRPBTC','EOSBTC','ADABTC','TRXBTC','LTCBTC','BCHABCBTC','BCHSVBTC','BCCBTC','HSRBTC','ICNBTC','VENBTC','WINGSBTC','TRIGBTC','CHATBTC','RPXBTC','CLOAKBTC','BCNBTC','PAXBTC','USDCBTC']

    for symbol in symbols:
        if symbol['symbol'][-3:] == quote_currency and symbol['symbol'] not in symbol_blacklist:
            symbol_list.append(symbol['symbol'])
    return symbol_list

def get_historical_klines_df(symbol, interval, from_date, to_date = None):
    if to_date is None:
        klines = client.get_historical_klines(symbol, interval, from_date)
    else:
        klines = client.get_historical_klines(symbol, interval, from_date, to_date)
    df = pd.DataFrame(klines)
    df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume','ignore']
    # df.open_time = pd.to_datetime(df.open_time, unit='ms')
    # df.close_time = pd.to_datetime(df.close_time, unit='ms')
    # df = df.set_index('open_time')
    return df

def get_klines_df(symbol, interval):
    klines = client.get_klines(symbol=symbol, interval=interval)
    df = pd.DataFrame(klines)
    df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume','ignore']
    # df.open_time = pd.to_datetime(df.open_time, unit='ms')
    # df.close_time = pd.to_datetime(df.close_time, unit='ms')
    # df = df.set_index('open_time')
    return df

# if isQuoteAsset = True, enter quantity in BTC
def calc_market_order_qty(symbol, input_asset_size, isQuoteAsset=True):
    # minimum transact quantity
    lot_size = float(client.get_symbol_info(symbol)['filters'][2]['minQty'])
    
    # symbol - e.g. 'BNBBTC'
    # quote asset size - e.g. 0.4 btc, positive = buy, negative = sell
    if input_asset_size > 0: depth = client.get_order_book(symbol=symbol)['asks']
    elif input_asset_size < 0: depth = client.get_order_book(symbol=symbol)['bids']
    
    input_asset_size_balance = abs(input_asset_size)    # deduct until zero
    cumulative_size = 0    # sum order book base_asset size across price levels

    # return these variable
    base_asset_size = 0    
    price = 0
        
    if isQuoteAsset:
        for level in depth:
            level_quote_asset = float(level[0]) * float(level[1])    # btc size of 1 price level
            cumulative_size += level_quote_asset
            if cumulative_size > abs(input_asset_size):
                price = float(level[0])
                base_asset_size += round(input_asset_size_balance / float(level[0]), 8)
                break
            input_asset_size_balance -= level_quote_asset
            base_asset_size += float(level[1])
    else:
        for level in depth:
            level_base_asset = float(level[1])    # btc size of 1 price level
            cumulative_size += level_base_asset
            if cumulative_size > abs(input_asset_size):
                price = float(level[0])
                base_asset_size += input_asset_size_balance
                break
            input_asset_size_balance -= level_base_asset
            base_asset_size += float(level[1])
        
    base_asset_size = abs(base_asset_size)
    base_asset_size = round(math.floor(base_asset_size / lot_size) * lot_size, 8)
    price = '{price:.8f}'.format(price=price)    # format price as 8-decimal string
    return (price, base_asset_size)


def calc_limit_order_qty(symbol, price, input_asset_size, isQuoteAsset=True):
    # minimum transact quantity
    lot_size = float(client.get_symbol_info(symbol)['filters'][2]['minQty'])
    input_asset_size = float(input_asset_size)
    price = float(price)

    # return these variable
    base_asset_size = 0    
        
    if isQuoteAsset:
        base_asset_size = input_asset_size / price
    else:
        base_asset_size = input_asset_size
        
    base_asset_size = abs(base_asset_size)
    base_asset_size = round(math.floor(base_asset_size / lot_size) * lot_size, 8)
    return base_asset_size

def format_string_price(symbol, stop_price, offset):
    # offset = allowed slippage percentage, negative for sell orders
    # offset will be applied to stop_price to calculate limit price for stop-limit order
    tick_size_inverted = 1 / float(client.get_symbol_info(symbol)['filters'][0]['tickSize'])
    
    stop_price = float(stop_price)
    
    price = math.floor((stop_price * (1 + offset)) * tick_size_inverted) / tick_size_inverted
    price = '{price:.8f}'.format(price=price)    # format price as 8-decimal string
    
    return price


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_trade_notif_email(toAddr, subject, body):
    fromAddr = env("EMAIL_USER")
    msg = MIMEMultipart()
    msg['From'] = fromAddr
    msg['To'] = toAddr
    msg['Subject'] = subject
    body = body
    msg.attach(MIMEText(body, 'plain'))
    
    server = smtplib.SMTP('smtp.office365.com', 587)
    server.ehlo()
    server.starttls()

    #Next, log in to the server
    server.login(fromAddr, env("EMAIL_PW"))

    #Send the mail
    text = msg.as_string()
    server.sendmail(fromAddr, toAddr, text)
    server.quit()


def unix_timestamp_diff_minutes(start_time, end_time):
    minutes = (datetime.utcfromtimestamp(end_time/1000) - datetime.utcfromtimestamp(start_time/1000)).total_seconds()/60
    return minutes

