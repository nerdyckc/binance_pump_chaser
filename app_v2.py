from environs import Env
env = Env()
env.read_env()
api_key=env("BINANCE_APIKEY")
api_secret=env("BINANCE_APISECRET")

import talib
import signal
from datetime import datetime
from time import sleep
import numpy as np
import pandas as pd

from utils import get_symbols, get_klines_df, calc_market_order_qty, calc_limit_order_qty, format_string_price, send_trade_notif_email, unix_timestamp_diff_minutes

from binance.client import Client
client = Client(api_key, api_secret)
from binance.enums import *
from binance.websockets import BinanceSocketManager
bm = BinanceSocketManager(client)

KLINE_INTERVAL = KLINE_INTERVAL_1MINUTE
TRADE_SIZE = 0.01    # btc allocated per buy trigger
msg_count = 0
prev_high_lookback_days = 7
SLIPPAGE_ALLOWED = 0.007
INITIAL_STOPLOSS = 0.95

# **********************************************
# ************ START of handle_data ************
# **********************************************
def handle_data(candle):
    global conn_key, conn_key_user, currentTime, msg_count
    candle = candle['data']
    symbol = candle['s']
    length = len(market_data_db[symbol])
    msg_count += 1

    # 1m candle streams - SELL order management
    if candle['k']['i'] == '1m' and symbol in strategy_pos_db and strategy_pos_db[symbol]['order_status'] == 'FILLED':
        last_price = float(candle['k']['c'])
        if last_price > strategy_pos_db[symbol]['high_water_mark']:
            strategy_pos_db[symbol]['high_water_mark'] = last_price
            print('{symbol} updated high_water_mark to {high:.8f}'.format(symbol=symbol, high=last_price))
        if strategy_pos_db[symbol]['sell_orderId'] is None:     # not yet place stop_loss order
            sell_stop_price = float(strategy_pos_db[symbol]['cost_basis']) * INITIAL_STOPLOSS
            strategy_pos_db[symbol]['sell_stop_price'] = format_string_price(symbol, sell_stop_price, 0.00)
            limit_price = format_string_price(symbol, sell_stop_price, offset=-SLIPPAGE_ALLOWED)    # apply 0.5% offset to sell_stop_price to get sell limit price
            print('{} new position - place stop loss order'.format(symbol))
            sell_order = client.create_order(
                symbol=symbol,
                side=SIDE_SELL,
                type=ORDER_TYPE_STOP_LOSS_LIMIT,
                timeInForce=TIME_IN_FORCE_GTC,
                quantity=strategy_pos_db[symbol]['qty'],
                stopPrice=strategy_pos_db[symbol]['sell_stop_price'],
                price=limit_price)
            strategy_pos_db[symbol]['sell_orderId'] = sell_order['orderId']     # update position table sell_orderId
        elif strategy_pos_db[symbol]['sell_orderId'] is not None:     # stop_loss order already placed, check if need to raise stop-loss price
            pct_gain = (last_price / float(strategy_pos_db[symbol]['cost_basis'])) - 1
            if pct_gain >= 0.1:
                # stop price trailing percent before update
                if pct_gain >= 0.1 and pct_gain < 0.18:         pct_trail = 0.900
                elif pct_gain >= 0.18 and pct_gain < 0.26:      pct_trail = 0.910
                elif pct_gain >= 0.26 and pct_gain < 0.33:      pct_trail = 0.920
                elif pct_gain >= 0.33 and pct_gain < 0.40:      pct_trail = 0.933
                elif pct_gain >= 0.40:                          pct_trail = 0.946
                sell_stop_price = last_price * pct_trail
                pct_stop_diff = (sell_stop_price / float(strategy_pos_db[symbol]['sell_stop_price'])) - 1
                if pct_stop_diff >= 0.01:   # update stop price if change is greater than 1%
                    # cancel current sell order, create new one using new sell_stop_price
                    client.cancel_order(symbol=symbol, orderId=strategy_pos_db[symbol]['sell_orderId'])
                    strategy_pos_db[symbol]['sell_orderId'] = None
                    strategy_pos_db[symbol]['sell_stop_price'] = format_string_price(symbol, sell_stop_price, 0.00)
                    limit_price = format_string_price(symbol, sell_stop_price, offset=-SLIPPAGE_ALLOWED)    # apply 0.5% offset to sell_stop_price to get sell limit price
                    print('{} existing position - update stop loss price'.format(symbol))
                    sell_order = client.create_order(
                        symbol=symbol,
                        side=SIDE_SELL,
                        type=ORDER_TYPE_STOP_LOSS_LIMIT,
                        timeInForce=TIME_IN_FORCE_GTC,
                        quantity=strategy_pos_db[symbol]['qty'],
                        stopPrice=strategy_pos_db[symbol]['sell_stop_price'],
                        price=limit_price)
                    strategy_pos_db[symbol]['sell_orderId'] = sell_order['orderId']     # update position table sell_orderId

    # 1d candle streams - update lookback_period_high every 24 hours
    if candle['k']['i'] == '1d' and candle['k']['x']:
        daily_data = get_klines_df(symbol, '1d').tail(prev_high_lookback_days)
        lookback_period_high_db[symbol] = float(daily_data.head(prev_high_lookback_days-1)['high'].max())
        

    # 1m candle streams - include BUY logic
    if candle['k']['i'] == '1m' and candle['k']['x']:
        market_data_db[symbol].loc[length] = [candle['k']['t'], candle['k']['o'], candle['k']['h'], candle['k']['l'], candle['k']['c'], candle['k']['v'], candle['k']['T'], candle['k']['q'], candle['k']['n'], candle['k']['V'], candle['k']['Q'], candle['k']['B']]

        if candle['k']['t'] > currentTime:
            currentTime = candle['k']['t']
            print('>>>>> Current time:', datetime.utcfromtimestamp(currentTime/1000).strftime('%Y-%m-%d %H:%M:%S'))
            if currentTime % 900000 == 0:   # every 15 minutes
                for pos in strategy_pos_db:
                    print(pos, strategy_pos_db[pos])

        close = pd.to_numeric(market_data_db[symbol]['close']).values
        opened = pd.to_numeric(market_data_db[symbol]['open']).values
        high = pd.to_numeric(market_data_db[symbol]['high']).values[-121:-1]
        low = pd.to_numeric(market_data_db[symbol]['low']).values[-121:-1]
        prev_high_X_minute = high.max()
        prev_low_X_minute = low.max()
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
        minBtcTurnover3 = (volume[-1] >= 14.0)
        minVRatio = (v_ratio >= 10)
        minNumTrades = (num_trades[-1] >= 450)
        minTradeRatio = (num_trades_ratio >= 10)
        
        open_orders = client.get_open_orders(symbol=symbol)

        if len(open_orders):
            # if stop-buy order open for 60 minutes, assume breakout failed, cancel order
            time_elapsed = unix_timestamp_diff_minutes(start_time=strategy_pos_db[symbol]['transactTime'], end_time=candle['E'])
            if open_orders[0]['side'] == 'BUY' and float(open_orders[0]['executedQty']) == 0 and time_elapsed >= 60:
                result = client.cancel_order(symbol=symbol, orderId=strategy_pos_db[symbol]['orderId'])
                del strategy_pos_db[symbol]
                print('{} Cancelled buy-stop order after 60 minutes\n{}'.format(symbol, result))

        if minVRatio and isGreenCandle and isPriceBreakout and minBtcTurnover1:
            print('{symbol:9} {price:.8f}\t{pct_chg:6.4f}%  {volume:6.3f}  {volume_120ma:6.3f}  v_ratio:{v_ratio:.2f}  no. trades:{num_trades:3d}  avg trades:{avg_trades:3.0f}  7d_high:{prev_high:.8f}'.format(
                symbol=symbol, price=close[-1], pct_chg=pct_chg_1period, volume=volume[-1], volume_120ma=volume_120ma , v_ratio=v_ratio, 
                num_trades=num_trades[-1], avg_trades=num_trades_120ma, prev_high=lookback_period_high_db[symbol]
                    ))
                    
            # check there is no position and no open orders
            isFlatPosition = (symbol not in strategy_pos_db) and not (len(open_orders))     
            
            if minBtcTurnover2 and minNumTrades and minTradeRatio and isFlatPosition:
                if close[-1] < lookback_period_high_db[symbol]:
                    # initiate a placeholder object when order is filled (logic in handle_orders function)
                    strategy_pos_db[symbol] = {
                        'orderId': None,
                        'clientOrderId': None,
                        'transactTime': None,
                        'order_status': None,
                        'cost_basis': None,
                        'qty': 0,
                        'high_water_mark': 0,
                        'sell_orderId': None,
                        'sell_stop_price': None
                    }
                    print('******** PLACE STOP BUY ORDER {symbol:9} {prev_high:.8f}'.format(symbol=symbol, prev_high=lookback_period_high_db[symbol]))
                    buy_limit_price = format_string_price(symbol, stop_price=lookback_period_high_db[symbol], offset=SLIPPAGE_ALLOWED)
                    buy_stop_price = format_string_price(symbol, lookback_period_high_db[symbol], 0.00)
                    base_asset_qty = calc_limit_order_qty(symbol, buy_limit_price, TRADE_SIZE)
                    buy_order = client.create_order(
                                    symbol=symbol,
                                    side=SIDE_BUY,
                                    type=ORDER_TYPE_STOP_LOSS_LIMIT,
                                    timeInForce=TIME_IN_FORCE_GTC,
                                    quantity=base_asset_qty,
                                    stopPrice=buy_stop_price,
                                    price=buy_limit_price)
                    strategy_pos_db[symbol]['orderId'] = buy_order['orderId']
                    strategy_pos_db[symbol]['clientOrderId'] = buy_order['clientOrderId']
                    strategy_pos_db[symbol]['transactTime'] = buy_order['transactTime']
                elif minBtcTurnover3 and close[-1] >= lookback_period_high_db[symbol]:   # price above 7-day high, market buy
                    # initiate a placeholder object when order is filled (logic in handle_orders function)
                    strategy_pos_db[symbol] = {
                        'orderId': None,
                        'clientOrderId': None,
                        'transactTime': None,
                        'order_status': None,
                        'cost_basis': None,
                        'qty': 0,
                        'high_water_mark': 0,
                        'sell_orderId': None,
                        'sell_stop_price': None
                    }
                    print('******** MARKET BUY!')
                    price, size = calc_market_order_qty(symbol, TRADE_SIZE, isQuoteAsset=True)
                    buy_order = client.order_limit_buy(
                                    symbol=symbol,
                                    quantity=size,
                                    price=price)
                    strategy_pos_db[symbol]['orderId'] = buy_order['orderId']
                    strategy_pos_db[symbol]['clientOrderId'] = buy_order['clientOrderId']
                    strategy_pos_db[symbol]['transactTime'] = buy_order['transactTime']
        
        # bug fix - truncate dataframe rows to preserve memory
        if length >= 180:
            market_data_db[symbol] = market_data_db[symbol].tail(130).reset_index(drop=True)


    event_time_second = int(datetime.utcfromtimestamp(candle['E']/1000).strftime('%S'))    
    if msg_count > 30000 and event_time_second >=26 and event_time_second <=50:
        # close and restart the socket
        print('restart socket ' + datetime.utcfromtimestamp(client.get_server_time()['serverTime']/1000).strftime('%H:%M:%S'))
        bm.stop_socket(conn_key)
        bm.stop_socket(conn_key_user)
        bm.close()
        conn_key = bm.start_multiplex_socket(symbol_stream_name, handle_data)
        conn_key_user = bm.start_user_socket(handle_orders)
        msg_count = 0
# ********************************************        
# ************ END of handle_data ************
# ********************************************


# **********************************************
# *********** START of handle_orders ***********
# **********************************************
def handle_orders(msg):
    symbol = msg['s']
    if msg['e'] == 'executionReport':
        print('test', msg)
        print('calling strategy_pos_db in handle_orders', strategy_pos_db)
        if msg['S'] == 'BUY':
            sleep(0.15)  # Time in seconds.
            if msg['X'] == 'FILLED': print(symbol, 'order filled', strategy_pos_db[symbol]['orderId'], msg['i'])
            if msg['X'] == 'FILLED' and symbol in strategy_pos_db and strategy_pos_db[symbol]['orderId'] == msg['i']:
                print('success if you see this line')
                cost_basis = float(msg['Z']) / float(msg['z'])
                # record keeping
                strategy_pos_db[symbol] = {
                    'orderId': msg['i'],
                    'clientOrderId': msg['c'],
                    'transactTime': msg['E'],
                    'order_status': msg['X'],   # update order_status to 'FILLED'
                    'cost_basis': cost_basis,
                    'qty': float(msg['z']),
                    'high_water_mark': 0,
                    'sell_orderId': None,
                    'sell_stop_price': None
                }

                print('end of handle_orders', strategy_pos_db[symbol])
                
                subject = 'Binance trade alert - bot {symbol:9} @ {price}'.format(symbol=symbol, price=strategy_pos_db[symbol]['cost_basis'])
                body = 'Trade Size: {}btc'.format(TRADE_SIZE)
                send_trade_notif_email('ck.chan@sonivy.com', subject, body)

            if msg['X'] == 'CANCELED' and symbol in strategy_pos_db and strategy_pos_db[symbol]['orderId'] == msg['i']:
                # buy order cancelled
                del strategy_pos_db[symbol]
        
        if msg['S'] == 'SELL':
            if msg['X'] == 'FILLED' and symbol in strategy_pos_db:
                sell_cost_basis = float(msg['Z']) / float(msg['z'])
                print('{symbol} closed position @ {sell_price:.8f} vs cost_basis {buy_price:.8f}'.format(
                    symbol=symbol, sell_price=sell_cost_basis, buy_price=strategy_pos_db[symbol]['cost_basis'])
                )
                del strategy_pos_db[symbol]

# **********************************************
# ************ END of handle_orders ************
# **********************************************

server_time = client.get_server_time()['serverTime']
currentTime = server_time
print('Server time', datetime.utcfromtimestamp(server_time/1000).strftime('%Y-%m-%d %H:%M:%S'))
market_data_db = dict()
strategy_pos_db = dict()
lookback_period_high_db = dict()

# fetch all symbols
symbols = get_symbols('BTC')
symbol_stream_name = []
for i, sym in enumerate(symbols):
    # two stream types: 1minute and 1day
    symbol_stream_name.append(sym.lower() + '@kline_' + KLINE_INTERVAL)
    symbol_stream_name.append(sym.lower() + '@kline_' + '1d')

for sym in symbols:
    # fetch most recent 121 rows, drop the last row (in effect keep 120 rows)
    market_data_db[sym] = get_klines_df(sym, KLINE_INTERVAL).tail(121).head(120)
    daily_data = get_klines_df(sym, '1d').tail(prev_high_lookback_days)
    lookback_period_high_db[sym] = float(daily_data['high'].head(prev_high_lookback_days-1).max())     # exclude current day high

conn_key = bm.start_multiplex_socket(symbol_stream_name, handle_data)
conn_key_user = bm.start_user_socket(handle_orders)

try:
    bm.start()
    # without this line, the main process is jumping out of the try...except 
    # block too early so the KeyboardInterrupt is not caught
    signal.pause()
except (KeyboardInterrupt, SystemExit):
    # when you need to exit
    bm.stop_socket(conn_key)
    bm.stop_socket(conn_key_user)
    bm.close()
    print('>>>>> closing socket on KeyboardInterrupt')

