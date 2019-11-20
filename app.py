from environs import Env
env = Env()
env.read_env()
api_key=env("BINANCE_APIKEY")
api_secret=env("BINANCE_APISECRET")

import signal
from datetime import datetime
import numpy as np
import pandas as pd

from utils import get_symbols, get_klines_df, calc_order_qty, send_trade_notif_email

from binance.client import Client
client = Client(api_key, api_secret)
from binance.enums import *
from binance.websockets import BinanceSocketManager
bm = BinanceSocketManager(client)

KLINE_INTERVAL = KLINE_INTERVAL_1MINUTE
TRADE_SIZE = 0.1    # btc allocated per buy trigger
msg_count = 0

def handle_data(candle):
    global conn_key, currentTime, msg_count
    candle = candle['data']
    symbol = candle['s']
    length = len(market_data_db[symbol])
    msg_count += 1

    if candle['k']['x']:
        # process message normally    
        market_data_db[symbol].loc[length] = [candle['k']['t'], candle['k']['o'], candle['k']['h'], candle['k']['l'], candle['k']['c'], candle['k']['v'], candle['k']['T'], candle['k']['q'], candle['k']['n'], candle['k']['V'], candle['k']['Q'], candle['k']['B']]

        if candle['k']['t'] > currentTime:
            currentTime = candle['k']['t']
            print('>>>>> Current time:', datetime.utcfromtimestamp(currentTime/1000).strftime('%Y-%m-%d %H:%M:%S'))

        lookback_minutes = 45

        close = pd.to_numeric(market_data_db[symbol]['close']).values
        opened = pd.to_numeric(market_data_db[symbol]['open']).values
        high = pd.to_numeric(market_data_db[symbol]['high']).values[-121:-1]
        low = pd.to_numeric(market_data_db[symbol]['low']).values[-121:-1]
        prev_high_X_minute = high.max()
        prev_low_X_minute = low.max()
        prev_X_minute_range = (high[-lookback_minutes:].max() - low[-lookback_minutes:].min())
        candle_to_prev_range_ratio = (close[-1] - opened[-1]) / prev_X_minute_range
        volume_120ma = pd.to_numeric(market_data_db[symbol]['quote_asset_volume']).values[-120:].mean()   # btc volume
        volume = pd.to_numeric(market_data_db[symbol]['quote_asset_volume']).values
        v_ratio = volume[-1]/volume_120ma
        pct_chg_1period = (close[-1] / close[-2] - 1) * 100
        pct_chg_2period = (close[-1] / close[-3] - 1) * 100
        num_trades_120ma = pd.to_numeric(market_data_db[symbol]['number_of_trades']).values[-120:].mean()
        num_trades = pd.to_numeric(market_data_db[symbol]['number_of_trades']).values
        num_trades_ratio = num_trades[-1] / num_trades_120ma
        

        # print('{symbol:9} {v_ratio:6.2f} {price:.8f} {open:.8f} {volume:.3f} df_length:{length}'.format(
        #     symbol=symbol, v_ratio=v_ratio, price=close[-1], open=opened[-1], volume=volume[-1], length=length
        #         ))
        isGreenCandle = (close[-1] > opened[-1])
        isPriceBreakout = (close[-1] > prev_high_X_minute)
        minBtcTurnover1 = (volume[-1] >= 3.0)
        minBtcTurnover2 = (volume[-1] >= 7.0)
        minVRatio = (v_ratio >= 10)
        minNumTrades = (num_trades[-1] >= 450)
        minTradeRatio = (num_trades_ratio >= 20)    # this ratio will be low is candle is in late stage of pump
        isFlatPosition = (symbol not in strategy_pos_db)
        notHighChase = (candle_to_prev_range_ratio >= 0.60)  # further verify candle breakout is in early stage

        if minVRatio and isGreenCandle and isPriceBreakout and minBtcTurnover1:
            print('{symbol:9}\t{price:.8f}\t{pct_chg:6.4f}%\t{volume:6.3f}\t{volume_120ma:6.3f}\tv_ratio:{v_ratio:.2f}\tno. trades:{num_trades:3d}\tavg no. trades:{avg_trades:3.0f}\trange_ratio:{range_ratio:.2f}'.format(
                symbol=symbol, price=close[-1], pct_chg=pct_chg_1period, volume=volume[-1], volume_120ma=volume_120ma , v_ratio=v_ratio, 
                num_trades=num_trades[-1], avg_trades=num_trades_120ma, range_ratio=candle_to_prev_range_ratio
                    ))
            if minBtcTurnover2 and minNumTrades and minTradeRatio and isFlatPosition and notHighChase:
                print('******** BUY {symbol:9} range_ratio:{range_ratio:.2f}'.format(symbol=symbol, range_ratio=candle_to_prev_range_ratio))
                # price, size = calc_order_qty(symbol, TRADE_SIZE, isQuoteAsset=True)
                # buy_order = client.order_limit_buy(
                #                 symbol=symbol,
                #                 quantity=size,
                #                 price=price)

                # strategy_pos_db[symbol] = { 'cost_basis': buy_order['price'], 'qty': size }

                # subject = 'Binance trade alert - bot {symbol:9} @ {price}'.format(symbol=symbol, price=buy_order['price'])
                # print('**********', subject, '**********')
                # body = 'Trade Size: 0.2btc'
                # send_trade_notif_email('ck.chan@sonivy.com', subject, body)
        
        # bug fix - truncate dataframe rows to preserve memory
        if length >= 180:
            market_data_db[symbol] = market_data_db[symbol].tail(130).reset_index(drop=True)

    event_time_second = int(datetime.utcfromtimestamp(candle['E']/1000).strftime('%S'))    
    if msg_count > 30000 and event_time_second >=26 and event_time_second <=50:
        # close and restart the socket
        print('restart socket ' + datetime.utcfromtimestamp(client.get_server_time()['serverTime']/1000).strftime('%H:%M:%S'))
        bm.stop_socket(conn_key)
        bm.close()
        conn_key = bm.start_multiplex_socket(symbol_stream_name, handle_data)
        msg_count = 0

server_time = client.get_server_time()['serverTime']
currentTime = server_time
print('Server time', datetime.utcfromtimestamp(server_time/1000).strftime('%Y-%m-%d %H:%M:%S'))
market_data_db = dict()
strategy_pos_db = dict()

# fetch all symbols
symbols = get_symbols('BTC')
symbol_stream_name = []
for i, sym in enumerate(symbols):
    symbol_stream_name.append(sym.lower() + '@kline_' + KLINE_INTERVAL)

for sym in symbols:
    # fetch most recent 121 rows, drop the last row (in effect keep 120 rows)
    market_data_db[sym] = get_klines_df(sym, KLINE_INTERVAL).tail(121).head(120)
conn_key = bm.start_multiplex_socket(symbol_stream_name, handle_data)


try:
    bm.start()
    # without this line, the main process is jumping out of the try...except 
    # block too early so the KeyboardInterrupt is not caught
    signal.pause()
except (KeyboardInterrupt, SystemExit):
    # when you need to exit
    bm.stop_socket(conn_key)
    bm.close()
    print('>>>>> closing socket on KeyboardInterrupt')
