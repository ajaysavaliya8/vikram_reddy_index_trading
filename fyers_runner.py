import os
from time import sleep
from threading import Thread
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import pandas as pd
from datetime import datetime, timedelta, date
from datetime import time as Time
import numpy as np
import os
from urllib.parse import parse_qs, urlparse
import math
import base64
# from config import fyers_retrive_config  
from models import fetch_credentials_by_userId
from time import sleep
import time
import json
import math
import logging
from threading import Thread, Event , Lock
import os
import pickle
import asyncio
from collections import deque
from asyncio import Lock as async_lock
from typing import Dict
from ShoonyaApi.api_helper import ShoonyaApiPy
import pyotp
from fastapi import FastAPI,Request,WebSocket,WebSocketDisconnect,status,HTTPException,Depends,Query,Form
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import requests

class FyersRunner(Thread):

    def __init__(self, broker , thread_name, index , pass_msg_to_queue , lot , expiry , primum_profit_point , max_drawdown , primum_trail_sl_activation_point , ce_strike_distance , pe_strike_distance  ,
                 premium_sl_point , ce , pe , timeframe , entry_type , sl_type , sl ,  tp_type  ,    master_df , shoonya , fyers , angleone ,  total_booked_pl ,  update_booked_pl ,  load_order , logger ):
        Thread.__init__(self, name=thread_name + index)
        self.lock = Lock()
        
        self.shoonya = shoonya
        self.fyers  = fyers
        self.angleone = angleone
        self.broker = broker
        self.update_booked_pl = update_booked_pl
        self.load_order = load_order
        self.logger = logger
        self.tradingmode = "DEMO"

        self.lot = lot
        self.expiry = pd.to_datetime(expiry).date()

        self.max_drawdown = max_drawdown
        self.primum_trail_sl_activation_point = primum_trail_sl_activation_point
        self.ce_strike_distance = ce_strike_distance
        self.pe_strike_distance = pe_strike_distance

        self.ce = ce
        self.pe = pe
        self.timeframe = timeframe

        self.sl = sl
        self.sl_candle_number = None
  
        self.index = index

        self.stop_event = Event()

        self.index_ohlc_df = None
        self.finished = False

        self.execute_pe = False
        self.execute_ce = False
        self.partial_half_achived = False
        self.pass_msg_to_queue = pass_msg_to_queue
        self.future_price_at_order = None
 
        self.total_value = 0
        self.master_df = master_df
        self.close_qty = 0
        self.msg = {"index":self.index , "Current_State":"" ,  "Entered Symbol": ""  ,"Entered Price": None  , "Entered Qty": ""  , "Remaining Qty":  "" , "LTP": ""  ,   "Running P&L": "" ,
                    "FUTURE SL":  ""  , "PRIMUM TP": ""  ,
                    "PRIMUM SL": "" ,"premium_sl_point": premium_sl_point , "primum_profit_point":float(primum_profit_point) ,  "increased_sl_point": 0 ,
                    "TP Type": tp_type  , 
                    "SL Type": sl_type  , "Entry Type": entry_type , "Booked P&L": total_booked_pl,
                    "ce_ltp":0 , "pe_ltp":0 }
        self.lot_size = int( self.master_df[ (self.master_df["underlying_symbol"] == self.index ) & (self.master_df["instrument"] == "OPTIDX" ) ]["lot_size"].iloc[0] )
 

    def send_order_and_verify(self , broker , index , symbol , qty , side):
        with self.lock:
            if broker =="shoonya":
                try:
                    _response = self.shoonya.place_order(buy_or_sell=side.upper()[0] , product_type="M",
                                        exchange="NFO", tradingsymbol=symbol, 
                                        quantity=qty , discloseqty=0,price_type='MKT', price=0, trigger_price=0,
                                        retention= "DAY" , remarks='')
                    print(_response)
                    if _response['stat'] != 'Ok':
                        return False , None
                except Exception as e:
                    self.logger.warning(e)
                    self.stop_event.set()
                    self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"warning", "title":f"{index}"  , "msg": f"{index}  Order Placing Error {e}" })
                    return False , None
                else:
                    sleep(1)
                    for i in range(3):
                        try:
                            order_book = self.shoonya.get_order_book()
                            # print("order_book")
                            # print(order_book)
                            if not order_book:
                                sleep(i)
                                continue
                        except Exception as e:
                            print(e)
                            sleep(i)
                            continue
                        else:
                            for _order in order_book:
                                if _response['norenordno'] == _order['norenordno'] and _order['stat'] == "Ok":
                                    if _order["status"] ==  "COMPLETE" and _order["st_intrn"] == "COMPLETE":
                                        print(_order)
                                        order_price = float( _order['avgprc'])
                                        return True , order_price 
                return False , None

            elif broker == "angleone":
                orderparams = {
                    "variety":"NORMAL",
                    'ordertype': "MARKET",
                    'producttype': "INTRADAY",
                    'duration': 'DAY',
                    'price':"0" ,
                    'triggerprice': '0' ,
                    'quantity':  f"{qty}" ,
                    "tradingsymbol":symbol ,
                    "transactiontype":side,
                    "exchange":"NFO",
                    "symboltoken": self.master_df[self.master_df['symbol'] == symbol]['token'].iloc[0] ,
                    }

                try:
                    sl_response = self.angleone.placeOrderFullResponse(orderparams)
                    print("sl_response" )
                    print(sl_response)
                    if (sl_response['status']!=True) or (sl_response['message']!='SUCCESS'):
                        return False , None
                    sl_id = sl_response['data']['orderid']

                except Exception as e:
                    self.logger.warning(e)
                    self.stop_event.set()
                    self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"warning", "title":f"{index}"  , "msg": f"{index}  Order Placing Error {e}" })
                    return False , None
                else:
                    sleep(1)
                    for i in range(3):
                        try:
                            order_book = self.angleone.orderBook()
                            if (order_book['status'] != True )or (order_book[ 'message']!='SUCCESS'):
                                sleep(i+1)
                                continue
                        except Exception as e:
                            print(e)
                            sleep(i)
                            continue
                        else:
                            order_book_df = pd.DataFrame(order_book['data'])
                            order_book_df = order_book_df[order_book_df['orderid']==sl_id]
                            print(order_book_df['orderstatus'].iloc[0] , order_book_df['averageprice'].iloc[0])
                            return  order_book_df['orderstatus'].iloc[0] , order_book_df['averageprice'].iloc[0]

                return False , None

            elif broker == "fyers":
                
                try:
                    buy_order_details = self.fyers.place_order(data = {
                                                    "symbol": symbol ,
                                                    "qty":qty ,
                                                    "type":2,
                                                    "side":side,
                                                    "productType":"INTRADAY",
                                                    "limitPrice":0,
                                                    "stopPrice":0,
                                                    "validity":"DAY",
                                                    "disclosedQty":0,
                                                    "offlineOrder":False,
                                                    "orderTag":"tag1"
                                                    })
                    self.logger.info(f"buy_order_details: {buy_order_details}" )
                    sleep(0.3)
                except Exception as e:
                    self.logger.warning(e)
                    self.stop_event.set()
                    self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"warning", "title":f"{index}"  , "msg": f"{index}  Order Placing Error {e}" })
                    return False , None
                else:
                    if buy_order_details["s"] == "ok" and buy_order_details["code"] ==  1101:
                        for i in range(3):
                            try:
                                data = {"id":buy_order_details['id']}
                                ob = self.fyers.orderbook(data=data)
                                self.logger.info(f"ob : {ob}")
                                if ob['s']=='ok':
                                    orderBook = ob['orderBook']
                                    for order in orderBook:
                                        if order['id']== buy_order_details['id']:
                                            if order['status']==2:
                                                self.logger.info(order)
                                                return True , order['tradedPrice']
                                            elif order['status']==1 or order['status']==5 or order['status']==7:
                                                return False , None
                                            elif order['status'] == 6:
                                                sleep(i)
                            except Exception as e:
                                print(e)
                                sleep(i)
                    return False , None
        
    def update_values(self , primum_profit_point ,max_drawdown , primum_trail_sl_activation_point , premium_sl_point ):
        with self.lock:
            self.msg['primum_profit_point'] = float(primum_profit_point)
            self.logger.info(f"updating value primum_profit_point = {self.msg['primum_profit_point']}")
            
            self.max_drawdown = self.msg['max_drawdown'] = max_drawdown
            self.logger.info(f"updating value max_drawdown = {max_drawdown}")
            
            self.primum_trail_sl_activation_point = self.msg['primum_trail_sl_activation_point'] = primum_trail_sl_activation_point
            self.logger.info(f"updating value primum_trail_sl_activation_point = {primum_trail_sl_activation_point}")
            
            self.msg['premium_sl_point'] = premium_sl_point
            self.logger.info(f"updating value premium_sl_point = {premium_sl_point}")
            
            if self.msg["Entered Price"] is not None:
                self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                self.logger.info(f"updating value PRIMUM TP = {self.msg["PRIMUM TP"]}")
                self.msg["PRIMUM SL"] = round(self.msg["Entered Price"] - self.msg['premium_sl_point'] , 2)
            self.logger.info(f"updating value PRIMUM SL = {self.msg["PRIMUM SL"]}")


    def update_index_ohlc(self):
        
        SymbolTicker = self.master_df[(self.master_df['underlying_symbol'] == self.index) &  (self.master_df['instrument'] == "FUTIDX" ) ]['trading_symbol'].iloc[0]

        while not self.stop_event.is_set():
            sleep(0.7)
            try:
                ltp = self.master_df.loc[self.master_df['trading_symbol'] == SymbolTicker, 'ltp'].iloc[0] 
                current_time = datetime.now()
                if not self.index_ohlc_df is None:
                    if current_time < ( self.index_ohlc_df["time"].iloc[-1] + np.timedelta64(self.timeframe, "m") ):
                        if ltp > self.index_ohlc_df["high"].iloc[-1]: self.index_ohlc_df.loc[ self.index_ohlc_df.index[-1], "high" ] = ltp
                        elif ltp < self.index_ohlc_df["low"].iloc[-1]: 
                            self.index_ohlc_df.loc[ self.index_ohlc_df.index[-1], "low" ] = ltp
                        self.index_ohlc_df.loc[ self.index_ohlc_df.index[-1], "close" ] = ltp
                    elif current_time >= ( self.index_ohlc_df["time"].iloc[-1] + np.timedelta64(self.timeframe, "m") ):
                        self.index_ohlc_df.loc[len(self.index_ohlc_df.index)] = { "time": self.index_ohlc_df["time"].iloc[-1] + np.timedelta64(self.timeframe, "m"), "open": ltp, "high": ltp, "low": ltp, "close": ltp, }
                    # sleep(1)
                    # print(self.index_ohlc_df)
            except Exception as e:       
                self.logger.warning(f"Error in update_index_ohlc : {e}")

    def increment_sl(self, sl_mothod):
        with self.lock:
            if self.msg['premium_sl_point'] != 0:
                self.logger.warning(f"Increment_sl not worked premium_sl_point is not 0")
                return False

            if self.sl_candle_number is None or self.sl_candle_number +1 >  self.index_ohlc_df.index[-1]:
                self.logger.warning(f"Increment_sl not worked condition 2 False")
                return False
    
            self.msg["SL Type"] = sl_mothod
            self.logger.info(f"Increment_sl method {sl_mothod}")
    
            if self.sl_candle_number+1 ==  self.index_ohlc_df.index[-1]:
                if self.msg["SL Type"] == "HL":
                    if self.ce and self.execute_ce:
                        self.msg['FUTURE SL'] = self.index_ohlc_df["low"].iloc[self.sl_candle_number ]
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

                    elif self.pe and self.execute_pe:
                        self.msg['FUTURE SL'] = self.index_ohlc_df["high"].iloc[self.sl_candle_number]
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

                elif self.msg["SL Type"] == "50%":
                    if self.ce and self.execute_ce:
                        self.msg['FUTURE SL'] = round( ( self.index_ohlc_df["high"].iloc[self.sl_candle_number ] + self.index_ohlc_df["low"].iloc[self.sl_candle_number  ] ) / 2, 1)
                        self.sl_candle_number = self.sl_candle_number
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

                    elif self.pe and self.execute_pe:
                        self.msg['FUTURE SL'] = round( (     self.index_ohlc_df["high"].iloc[ self.sl_candle_number  ] + self.index_ohlc_df["low"].iloc[ self.sl_candle_number  ] ) / 2, 1 )
                        self.sl_candle_number  = self.sl_candle_number 
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True
            
            elif self.sl_candle_number+1 <  self.index_ohlc_df.index[-1]:
                if self.msg["SL Type"] == "HL":
                    if self.ce and self.execute_ce:
                        self.msg['FUTURE SL'] = self.index_ohlc_df["low"].iloc[self.sl_candle_number + 1]
                        self.sl_candle_number = self.sl_candle_number + 1
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

                    elif self.pe and self.execute_pe:
                        self.msg['FUTURE SL'] = self.index_ohlc_df["high"].iloc[self.sl_candle_number + 1]
                        self.sl_candle_number = self.sl_candle_number + 1
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

                elif self.msg["SL Type"] == "50%":
                    if self.ce and self.execute_ce:
                        self.msg['FUTURE SL'] = round( ( self.index_ohlc_df["high"].iloc[self.sl_candle_number + 1] + self.index_ohlc_df["low"].iloc[self.sl_candle_number + 1] ) / 2, 1)
                        self.sl_candle_number = self.sl_candle_number + 1
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

                    elif self.pe and self.execute_pe:
                        self.msg['FUTURE SL'] = round( (     self.index_ohlc_df["high"].iloc[ self.sl_candle_number + 1 ] + self.index_ohlc_df["low"].iloc[ self.sl_candle_number + 1 ] ) / 2, 1 )
                        self.sl_candle_number  = self.sl_candle_number + 1
                        self.logger.info(f"Future SL : {sl_mothod}")
                        return True

    def run(self):
        # for _ in range(3):
        # print(self.broker)
        if self.broker =="shoonya":
            try:
                lastBusDay = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                ret = self.shoonya.get_time_price_series(exchange='NFO', token= self.master_df[(self.master_df['underlying_symbol'] == self.index) & (self.master_df['instrument'] == 'FUTIDX')]['token'].iloc[0],
                                                            starttime=lastBusDay.timestamp(), interval= self.timeframe)
                self.index_ohlc_df = pd.DataFrame(ret)
                self.index_ohlc_df.rename(columns={'time': 'time', 'into': 'open', 'inth': 'high', 'intl': 'low', 'intc': 'close' ,'intv':'volume'}, inplace=True)
                self.index_ohlc_df =self.index_ohlc_df[["time", "open", "high", "low", "close" ]]
                self.index_ohlc_df[['open', 'high', 'low', 'close']] = self.index_ohlc_df[['open', 'high', 'low', 'close']].astype(float)
                self.index_ohlc_df["time"] = pd.to_datetime( self.index_ohlc_df["time"], format='%d-%m-%Y %H:%M:%S')
                self.index_ohlc_df = self.index_ohlc_df.sort_values(by="time", ascending=True)
                self.index_ohlc_df = self.index_ohlc_df.reset_index(drop=True)
                self.logger.info(self.index_ohlc_df)
            except Exception as e:
                self.logger.warning(f"error in OHLC data Downloading : {e}")
 

        elif self.broker =="fyers":
            try:
                data = {
                    "symbol": self.master_df[(self.master_df['underlying_symbol'] == self.index) &  (self.master_df['instrument'] == 'FUTIDX' ) ]['trading_symbol'].iloc[0] ,
                    "resolution": self.timeframe,
                    "date_format": "0",
                    "range_from": f'{int(time.mktime(time.strptime((datetime.now()-timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")))}',
                    "range_to": f'{int(time.mktime(time.strptime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")))}',
                    "cont_flag": "1",
                }
                response = self.fyers.history(data=data)
                self.index_ohlc_df = pd.DataFrame( response["candles"], columns=["time", "open", "high", "low", "close", "volume"], )
                self.index_ohlc_df = self.index_ohlc_df.drop("volume", axis=1)
                self.index_ohlc_df["time"] = pd.to_datetime( self.index_ohlc_df["time"], unit="s", utc=True)
                self.index_ohlc_df["time"] = self.index_ohlc_df["time"].dt.tz_convert("Asia/Kolkata")
                self.index_ohlc_df["time"] = self.index_ohlc_df["time"].dt.tz_localize(None)
                self.logger.info(self.index_ohlc_df)
            except Exception as e:
                self.logger.warning(f"error in OHLC data Downloading : {e}")
 


        elif self.broker =="angleone":
            try:
                if self.timeframe==1:
                    timeframe ="ONE_MINUTE" 
                if self.timeframe==3:
                    timeframe ="THREE_MINUTE" 
                if self.timeframe==5:
                    timeframe ="FIVE_MINUTE" 
                historicParam={
                "exchange": "NFO",
                "symboltoken": f'{self.master_df[(self.master_df['underlying_symbol'] == self.index) &  (self.master_df['instrument'] == 'FUTIDX' ) ]['token'].iloc[0]}',
                "interval": timeframe ,
                "fromdate": datetime.now().replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M") , 
                "todate":datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                data = self.angleone.getCandleData(historicParam)
                self.index_ohlc_df = pd.DataFrame( response["candles"], columns=["time", "open", "high", "low", "close", "volume"], )
                self.index_ohlc_df = self.index_ohlc_df.drop("volume", axis=1)
                self.index_ohlc_df["time"] = pd.to_datetime( self.index_ohlc_df["time"], unit="s", utc=True)
                self.index_ohlc_df["time"] = self.index_ohlc_df["time"].dt.tz_convert("Asia/Kolkata")
                self.index_ohlc_df["time"] = self.index_ohlc_df["time"].dt.tz_localize(None)
                self.logger.info(self.index_ohlc_df)
            except Exception as e:
                self.logger.warning(f"error in OHLC data Downloading : {e}")

        if self.index_ohlc_df is None or self.index_ohlc_df.empty:
            self.pass_msg_to_queue({ "display":"notification" ,"alert_type":"Error", "title":f"{self.index}" , "msg":f"Future OHLC DATA not loaded"})
            print(f"Future OHLC data Not Loded")
            self.stop_event.set()
            self.finished = True
            return

        print("self.index_ohlc_df")
        print(self.index_ohlc_df)
        Thread( target= self.update_index_ohlc , daemon=True).start()
        sleep(1)
        strike_different = 50 if self.index == "NIFTY" else 100

        atm_strike = round(self.index_ohlc_df["close"].iloc[-1] / strike_different ) * strike_different
        ce_strike = atm_strike + self.ce_strike_distance
        pe_strike = atm_strike - self.pe_strike_distance
        monitor_time = self.index_ohlc_df["time"].iloc[-1]

        self.msg["Current_State"] = "Monitoring"
 
        while not self.stop_event.is_set():
            sleep(1)
            if self.msg["Entry Type"] == "BREAK":
                if self.index_ohlc_df["time"].iloc[-1] > monitor_time:
                    self.stop_event.set()
                    self.pass_msg_to_queue({"display":"notification" ,"alert_type":"warning", f"title":f"{self.index}"  , "msg":f"Candle Not brake High Low"})
                    self.msg['Current_State'] = "Candle did not Break"
                    self.pass_msg_to_queue({"display":"table" ,  "msg":self.msg})
                    self.finished = True
                    return

                if self.ce:
                    if ( self.index_ohlc_df["high"].iloc[-2] < self.index_ohlc_df["close"].iloc[-1] ):
                        self.execute_ce = True
                        self.execute_pe = False
                        self.pe = False
                        self.pass_msg_to_queue({ "display":"notification" ,"alert_type":"success", "title":f"{self.index}"  , "msg":f"Trade in CE Allow"})
                        break

                if self.pe:
                    if ( self.index_ohlc_df["low"].iloc[-2] > self.index_ohlc_df["close"].iloc[-1] ):
                        self.execute_ce = False
                        self.execute_pe = True
                        self.ce = False
                        self.pass_msg_to_queue({ "display":"notification" ,"alert_type":"success", "title":f"{self.index}" , "msg":f"Trade in PE Allow"})
                        break

                self.pass_msg_to_queue({"display":"table" ,  "msg":self.msg})

            elif self.msg["Entry Type"] == "MARKET":
                if self.ce:
                    self.execute_ce = True
                    self.execute_pe = False
                elif self.pe:
                    self.execute_ce = False
                    self.execute_pe = True
                break                
            print( "p high", self.index_ohlc_df["high"].iloc[-2], " || ", " ltp ", self.index_ohlc_df["close"].iloc[-1], " || ", "p low", self.index_ohlc_df["low"].iloc[-2] )

        self.future_price_at_order = self.index_ohlc_df["close"].iloc[-1]
 
        self.ce_trading_symbol = self.master_df[ (self.master_df["strike"] == ce_strike) & (self.master_df["option_type"] == "CE") & (self.master_df["expiry"] == self.expiry) ]["trading_symbol"].iloc[-1]
        self.pe_trading_symbol = self.master_df[ (self.master_df["strike"] == pe_strike) & (self.master_df["option_type"] == "PE") & (self.master_df["expiry"] == self.expiry) ]["trading_symbol"].iloc[-1]

        if self.tradingmode == "DEMO":
            while not self.stop_event.is_set():
                if self.ce and self.execute_ce:
                    try:
                        self.msg['Entered Symbol'] = self.master_df[ (self.master_df["strike"] == ce_strike) & (self.master_df["option_type"] == "CE") & (self.master_df["expiry"] == self.expiry) ]["trading_symbol"].iloc[-1]
                        self.msg["Entered Price"] = self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0]
                        self.msg['FUTURE SL'] = self.index_ohlc_df["low"].iloc[-2]
                        self.sl_candle_number  = self.index_ohlc_df.index[-2]
                        self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                        self.msg["PRIMUM SL"] = round(self.msg["Entered Price"] - self.msg['premium_sl_point'] if self.msg["Entered Price"] - self.msg['premium_sl_point']> 0 else 0.5 , 2)
                        self.msg["Remaining Qty"] = self.msg["Entered Qty"] =  self.lot * self.lot_size
                        
                        self.load_order(self.tradingmode , "BUY"  , self.msg['Entered Symbol'] , self.msg["Remaining Qty"] , self.msg["Entered Price"] , "COMPLETED" )
                        break
                    except Exception as e:
                        self.logger.warning(e)
                        continue

                elif self.pe and self.execute_pe:
                    try:
                        self.msg['Entered Symbol'] = self.master_df[ (self.master_df["strike"] == pe_strike) & (self.master_df["option_type"] == "PE") & (self.master_df["expiry"] == self.expiry) ]["trading_symbol"].iloc[-1]
                        self.msg["Entered Price"] = self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0]
                        self.msg['FUTURE SL'] = self.index_ohlc_df["high"].iloc[-2]
                        self.sl_candle_number  = self.index_ohlc_df.index[-2]
                        self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                        self.msg["PRIMUM SL"] = round(self.msg["Entered Price"] - self.msg['premium_sl_point'] if self.msg["Entered Price"] - self.msg['premium_sl_point']> 0 else 0.5 , 2)
                        self.msg["Remaining Qty"] = self.msg["Entered Qty"] =  self.lot * self.lot_size
                        
                        self.load_order(self.tradingmode , "BUY"  , self.msg['Entered Symbol'] , self.msg["Remaining Qty"] ,self.msg["Entered Price"] , "COMPLETED" )
                        break
                    except Exception as e:
                        self.logger.warning(e)
                        continue


            if self.stop_event.is_set():
                return
            else:                
                self.msg["Current_State"] = "Activate"
                self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg":f"NIFTY  trading symbol :{ self.msg['Entered Symbol'] }"})

                self.msg["LTP"] = ltp = round(self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0] , 2)
                self.reference_buy_price = self.msg["Entered Price"]

                self.pass_msg_to_queue({"display":"table" , "msg":self.msg })
                
                self.total_value = self.msg["Remaining Qty"]
            # print( "buy_price= ", self.msg["Entered Price"] , "   buy_qty= ", self.msg["Remaining Qty"] , "  total_value= ", self.total_value )

            while not self.stop_event.is_set():
                sleep(0.5)
                self.msg["ce_ltp"] =  self.master_df.loc[self.master_df['trading_symbol'] == self.ce_trading_symbol, 'ltp'].iloc[0]
                self.msg["pe_ltp"] =  self.master_df.loc[self.master_df['trading_symbol'] == self.pe_trading_symbol, 'ltp'].iloc[0]
                
                self.msg["Running P&L"] =  round(( ltp - self.msg["Entered Price"] ) * self.msg["Remaining Qty"] , 2) 
                ltp = self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0]
                self.msg['LTP'] = ltp
                
                print( "max_drawdown:", self.max_drawdown, "total_value:", self.total_value , "current_profit_loss:", round(( ltp - self.msg["Entered Price"] ) * self.msg["Remaining Qty"] , 2) ,
                        "ltp:", round( ltp  , 2), "|" , "primum tp:" , self.msg["Entered Price"] + self.msg['primum_profit_point'] , "sl_activation:", self.primum_trail_sl_activation_point,  "sl:", self.msg['FUTURE SL'] ,
                        "future LTP : " ,self.index_ohlc_df["close"].iloc[-1]  )

                if self.primum_trail_sl_activation_point != 0:
                    # if ( self.ce and self.execute_ce ):
                    if self.reference_buy_price + self.primum_trail_sl_activation_point < self.msg["LTP"]:
                        print("self.reference_buy_price :" , self.reference_buy_price  , "self.primum_trail_sl_activation_point:",self.primum_trail_sl_activation_point , "ltp:" , self.msg["LTP"])
                        if self.msg['PRIMUM SL'] + self.primum_trail_sl_activation_point < self.msg["Entered Price"]:
                            self.reference_buy_price += self.primum_trail_sl_activation_point
                            self.msg["PRIMUM SL"] += round( self.primum_trail_sl_activation_point , 2)
                            self.pass_msg_to_queue( {  "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg":f"SL Trail to  :{self.reference_buy_price}"})
                        else:
                            self.reference_buy_price += self.primum_trail_sl_activation_point
                            self.msg["PRIMUM SL"] = round(self.msg["Entered Price"] + 0.5 , 2)
                            

                if (( ltp - self.msg["Entered Price"] ) * self.msg["Remaining Qty"]) < -1 * self.max_drawdown:
                    self.pass_msg_to_queue({ "display":"notification" ,"alert_type":"warning", "title":"NIFTY" ,   "msg":f"Max DrowDown Done"})
                    self.logger.info("max Drowdown done")
                    self.squreoff()
                    break
 
                if self.msg['premium_sl_point'] != 0:
                    if ltp < self.msg["PRIMUM SL"]:
                        self.pass_msg_to_queue({  "display":"notification" ,"alert_type":"warning", "title":"NIFTY" , "msg":f"Primum SL hit : {self.msg["PRIMUM SL"]}"})
                        self.logger.info("Primum SL hit")
                        self.squreoff()
                        break

                elif self.msg['premium_sl_point'] == 0:
                    if ( self.ce and self.execute_ce and self.index_ohlc_df["close"].iloc[-1] < self.msg['FUTURE SL'] ):
                        self.pass_msg_to_queue({  "display":"notification" ,"alert_type":"warning", "title":"NIFTY" , "msg":f"Future sl hit : { self.msg['FUTURE SL'] }"})
                        self.stop_event.set()
                        self.logger.info("Future sl hit")
                        self.squreoff()
                        break

                    if ( self.pe and self.execute_pe and self.index_ohlc_df["close"].iloc[-1] > self.msg['FUTURE SL'] ):
                        self.pass_msg_to_queue({  "display":"notification" ,"alert_type":"warning", "title":"NIFTY" ,  "msg":f"Future sl hit : { self.msg['FUTURE SL'] }"})
                        self.stop_event.set()
                        self.logger.info("Future sl hit")
                        self.squreoff()
                        break

                if ltp >  self.msg["Entered Price"] + self.msg['primum_profit_point'] :
                    # print(self.tp_type)
                    if self.msg["TP Type"] == "open":
                        pass
 
                    elif self.msg["TP Type"] == "partial":
                        if self.lot == 1:
                            self.stop_event.set()
                            self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 100% Profit at target : {str(self.msg['Entered Price'] + self.msg['primum_profit_point']) }" })
                            self.squreoff()
                            self.stop_event.set()
                            break

                        elif self.lot > 1:
                            if not self.partial_half_achived:

                                self.close_qty = int( math.ceil(abs(self.msg["Remaining Qty"]/self.lot_size)/2) * self.lot_size )
                                self.msg["Remaining Qty"] -= self.close_qty
                                self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 50% Profit at target : {str(self.msg['Entered Price'] + self.msg['primum_profit_point']) } , Qty:{self.close_qty}" })

                                self.partial_half_achived = True
                                self.msg['primum_profit_point'] += self.msg['primum_profit_point']
                                self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                                self.msg["Running P&L"] =  round((self.msg["Remaining Qty"] * ltp ) - self.total_value , 2)
                                self.logger.info(f"previous profit : {self.msg["Booked P&L"]}")
                                self.msg["Booked P&L"] = int(self.update_booked_pl( self.index , round( self.close_qty * (ltp - self.msg["Entered Price"] )  , 2 ) ))
                                
                                self.load_order( self.tradingmode , "SELL"  , self.msg['Entered Symbol'] , self.close_qty ,ltp , "COMPLETED" )
                                self.logger.info(f"current profit : {self.msg["Booked P&L"]}" )
                            else:
                                self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 50% Profit at target : {str(self.msg['Entered Price'] + self.msg['primum_profit_point']) } , Qty:{self.close_qty}" })
                                self.squreoff()
                                self.stop_event.set()
                                break

                    elif self.msg["TP Type"] == "full":
                        self.stop_event.set()
                        self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 100% Profit at target : {str(self.msg['Entered Price'] + self.msg['primum_profit_point']) }, Qty:{self.close_qty}" })
                        self.squreoff()
                        break
                if datetime.now().time() > Time(15, 15):
                    self.squreoff()
                    break    
                self.pass_msg_to_queue({"display":"table" , "msg":self.msg })


        elif self.tradingmode == "LIVE":
            
            while not self.stop_event.is_set():
                if self.ce and self.execute_ce:
                    symbol = self.master_df[ (self.master_df["strike"] == ce_strike) & (self.master_df["option_type"] == "CE") & (self.master_df["expiry"] == self.expiry) ]["trading_symbol"].iloc[-1]
                    qty = self.lot * self.lot_size
                    order_status , executed_price = self.send_order_and_verify(self.broker , self.index , symbol , qty  , "BUY")
                    if order_status:
                        try:
                            self.msg['Entered Symbol'] = symbol
                            self.msg["Remaining Qty"] = self.msg["Entered Qty"] =  qty
                            self.msg["Entered Price"] =  executed_price                        
                            self.msg['FUTURE SL'] = self.index_ohlc_df["low"].iloc[-2]
                            self.sl_candle_number  = self.index_ohlc_df.index[-2]
                            self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                            self.msg["PRIMUM SL"] = round(self.msg["Entered Price"] - self.msg['premium_sl_point'] if self.msg["Entered Price"] - self.msg['premium_sl_point']> 0 else 0.5 ,2)
                            self.load_order(self.tradingmode , "BUY"  , self.msg['Entered Symbol'] , self.msg["Remaining Qty"] , self.msg["Entered Price"] , "COMPLETED" )
                            break
                        except Exception as e:
                            self.logger.error(e)
                    else:
                        self.stop_event.set()
                        self.logger.warning("Order Sending Error")
                        self.squreoff()
                        return

                elif self.pe and self.execute_pe:
                    symbol =  self.master_df[ (self.master_df["strike"] == ce_strike) & (self.master_df["option_type"] == "PE") & (self.master_df["expiry"] == self.expiry) ]["trading_symbol"].iloc[-1]
                    qty = self.lot * self.lot_size
                    order_status , executed_price = self.send_order_and_verify(self.broker , self.index , symbol , qty  , "BUY")
                    if order_status:
                        try:
                            self.msg['Entered Symbol'] = symbol
                            self.msg["Entered Price"] =  executed_price
                            self.msg['FUTURE SL'] = self.index_ohlc_df["high"].iloc[-2]
                            self.sl_candle_number  = self.index_ohlc_df.index[-2]
                            self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                            self.msg["PRIMUM SL"] = round(self.msg["Entered Price"] - self.msg['premium_sl_point'] if self.msg["Entered Price"] - self.msg['premium_sl_point']> 0 else 0.5 , 2)
                            self.msg["Remaining Qty"] = self.msg["Entered Qty"] =  self.lot * self.lot_size
                            self.load_order(self.tradingmode , "BUY"  , self.msg['Entered Symbol'] , self.msg["Remaining Qty"] , self.msg["Entered Price"] , "COMPLETED" )
                            break
                        except Exception as e:
                            self.logger.error(e)
                    else:
                        self.stop_event.set()
                        self.logger.warning("Order Sending Error")
                        self.squreoff()
                        return

            self.msg["Current_State"] = "Activate"
            self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg":f"NIFTY  trading symbol :{ self.msg['Entered Symbol'] }"})

            self.msg["LTP"] = ltp = round(self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0] , 2 )
            self.reference_buy_price = self.msg["Entered Price"]

            self.pass_msg_to_queue({"display":"table" , "msg":self.msg })
            
            self.total_value = round( self.msg["Remaining Qty"] , 2)
            print( "buy_price= ", self.msg["Entered Price"] , "   buy_qty= ", self.msg["Remaining Qty"] , "  total_value= ", self.total_value )

            while not self.stop_event.is_set():
                sleep(0.5)
                self.msg["ce_ltp"] =  self.master_df.loc[self.master_df['trading_symbol'] == self.ce_trading_symbol, 'ltp'].iloc[0]
                self.msg["pe_ltp"] =  self.master_df.loc[self.master_df['trading_symbol'] == self.pe_trading_symbol, 'ltp'].iloc[0]

                self.msg["Running P&L"] =  round(( ltp - self.msg["Entered Price"] ) * self.msg["Remaining Qty"] , 2) 
                ltp = self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0]
                self.msg['LTP'] = ltp

                print( "max_drawdown:", self.max_drawdown, "total_value:", self.total_value , "current_profit_loss:", round(( ltp - self.msg["Entered Price"] ) * self.msg["Remaining Qty"] , 2) ,
                        "ltp:", round( ltp  , 2), "|" , "primum tp:" ,self.msg["Entered Price"] + self.msg['primum_profit_point'] , "sl_activation:", self.primum_trail_sl_activation_point,  "sl:", self.msg['FUTURE SL'] ,
                        "future LTP : " ,self.index_ohlc_df["close"].iloc[-1]  )

                if self.primum_trail_sl_activation_point != 0:
                    # if ( self.ce and self.execute_ce ):
                    if self.reference_buy_price + self.primum_trail_sl_activation_point < self.msg["LTP"]  and  self.msg['PRIMUM SL'] + self.primum_trail_sl_activation_point < self.msg["Entered Price"]:
                        self.reference_buy_price += self.primum_trail_sl_activation_point
                        self.msg["PRIMUM SL"] += self.primum_trail_sl_activation_point
                        self.pass_msg_to_queue( {  "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg":f"SL Trail to  :{self.reference_buy_price}"})
 

                if (( ltp - self.msg["Entered Price"] ) * self.msg["Remaining Qty"]) < -1 * self.max_drawdown:
                    self.pass_msg_to_queue({ "display":"notification" ,"alert_type":"warning", "title":"NIFTY" ,   "msg":f"Max DrowDown Done"})
                    self.logger.info("max Drowdown done")
                    self.squreoff()
                    break
 
                if self.msg['premium_sl_point'] != 0:
                    if ltp < self.msg["PRIMUM SL"]:
                        self.pass_msg_to_queue({  "display":"notification" ,"alert_type":"warning", "title":"NIFTY" , "msg":f"Primum SL hit : {self.msg["PRIMUM SL"]}"})
                        self.logger.info("Primum SL hit")
                        self.squreoff()
                        break

                elif self.msg['premium_sl_point'] == 0:
                    if ( self.ce and self.execute_ce and self.index_ohlc_df["close"].iloc[-1] < self.msg['FUTURE SL'] ):
                        self.pass_msg_to_queue({  "display":"notification" ,"alert_type":"warning", "title":"NIFTY" , "msg":f"Future sl hit : { self.msg['FUTURE SL'] }"})
                        self.stop_event.set()
                        self.logger.info("Future sl hit")
                        self.squreoff()
                        break

                    if ( self.pe and self.execute_pe and self.index_ohlc_df["close"].iloc[-1] > self.msg['FUTURE SL'] ):
                        self.pass_msg_to_queue({  "display":"notification" ,"alert_type":"warning", "title":"NIFTY" ,  "msg":f"Future sl hit : { self.msg['FUTURE SL'] }"})
                        self.stop_event.set()
                        self.logger.info("Future sl hit")
                        self.squreoff()
                        break

                if ltp >  self.msg["Entered Price"] + self.msg['primum_profit_point'] :
                    # print(self.tp_type)
                    if self.msg["TP Type"] == "open":
                        sleep(1)
                        pass
 
                    elif self.msg["TP Type"] == "partial":
                        if self.lot == 1:
                            self.stop_event.set()
                            self.squreoff()
                            self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 100% Profit at target : { self.msg["Entered Price"] + self.msg['primum_profit_point'] }" })
                            self.stop_event.set()
                            break
                        
                        elif self.lot > 1:
                            if not self.partial_half_achived:

                                self.close_qty = int( math.ceil(abs(self.msg["Remaining Qty"]/self.lot_size)/2) * self.lot_size )
                                partial_closed_staus , price = self.send_order_and_verify( self.broker , self.msg['index'] , self.msg['Entered Symbol']  , self.close_qty , "SELL" )
                                if partial_closed_staus:
                                    self.msg["Remaining Qty"] -= self.close_qty
                                    self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 50% Profit at target : { self.msg["Entered Price"] + self.msg['primum_profit_point'] } , Qty:{self.close_qty}" })
                                    self.partial_half_achived = True
                                    self.msg['primum_profit_point'] += self.msg['primum_profit_point']
                                    self.msg["PRIMUM TP"] = self.msg["Entered Price"] + self.msg['primum_profit_point']
                                    self.msg["Running P&L"] =  round((self.msg["Remaining Qty"] * ltp ) - self.total_value , 2)
                                    self.msg["Booked P&L"] = int(self.update_booked_pl( self.index , round( self.close_qty * (ltp - self.msg["Entered Price"] )  , 2 ) ))
                                    self.load_order( self.tradingmode , "SELL"  , self.msg['Entered Symbol'] , self.close_qty ,ltp , "COMPLETED" )
                                    self.logger.info(f"current profit : {self.msg["Booked P&L"]}" )
  
                            else:
                                self.squreoff()
                                self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 50% Profit at target : { self.msg["Entered Price"] + self.msg['primum_profit_point'] } , Qty:{self.close_qty}" })
                                self.stop_event.set()
                                break

                    elif self.msg["TP Type"] == "full":
                        self.stop_event.set()
                        self.squreoff()
                        self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"NIFTY  booked 100% Profit at target : { self.msg["Entered Price"] + self.msg['primum_profit_point']  } , Qty:{self.close_qty}" })
                        break
                if datetime.now().time() > Time(15, 14, 50):
                    self.squreoff()
                    break    
                self.pass_msg_to_queue({"display":"table" , "msg":self.msg })

            pass

        # self.pass_msg_to_queue({"display":"table" ,  "msg":self.msg})
        print("******************************************************")
        self.finished = True
        
 
    def squreoff(self):
        self.stop_event.set()
        sleep(1)
        self.logger.info( self.msg )

        try:
            if self.msg['Current_State'] == "Activate":
                if self.tradingmode =="DEMO" and self.msg["Remaining Qty"]>0:
                    ltp = self.master_df.loc[self.master_df['trading_symbol'] == self.msg['Entered Symbol'] , 'ltp'].iloc[0]
                    self.load_order(self.tradingmode , "SELL"  , self.msg['Entered Symbol'] , self.msg["Remaining Qty"] ,ltp , "COMPLETED" )
                    self.close_qty = self.msg["Remaining Qty"]
                    self.msg["Remaining Qty"] = 0
                    self.msg["Booked P&L"] = self.update_booked_pl( self.index , round( self.close_qty * (ltp - self.msg["Entered Price"]  )  , 2 ) )

                elif self.tradingmode =="LIVE" and self.msg["Remaining Qty"]>0:
                    squre_off_staus , price = self.send_order_and_verify( self.broker , self.msg['index'] , self.msg['Entered Symbol']  , self.msg["Remaining Qty"] , "SELL")
                    print("------------")
                    print(squre_off_staus , price , self.msg["Entered Price"])
                    if squre_off_staus:                    
                        self.load_order(self.tradingmode , "SELL"  , self.msg['Entered Symbol'] , self.msg["Remaining Qty"] , price , "COMPLETED" )
                        self.close_qty = self.msg["Remaining Qty"]
                        self.msg["Remaining Qty"] = 0
                        # print(f"previous profit" , self.msg["Booked P&L"])
                        self.msg["Booked P&L"] = self.update_booked_pl( self.index , round( self.close_qty * (price - self.msg["Entered Price"]  )  , 2 ) )
                        # print(f"current profit" , self.msg["Booked P&L"])
            
            self.msg['Current_State'] = "Closed"
            self.finished = True

            self.msg["Current_State"] =""
            self.msg["Entered Symbol"] = ""
            self.msg["Entered Price"] = ""
            self.msg["Entered Qty"] = ""
            self.msg["Remaining Qty"] =  ""
            self.msg["LTP"] = ""
            self.msg["Running P&L"] = ""
            self.msg["FUTURE SL"] =  ""
            self.msg["PRIMUM TP"] = ""
            self.msg["PRIMUM SL"] = ""
            self.msg["ce_ltp"] = ""
            self.msg["pe_ltp"] = ""
            self.pass_msg_to_queue( { "display":"notification" ,"alert_type":"success", "title":"NIFTY"  , "msg": f"Trade Closed" })
            self.logger.info("Order SquareOff Process Completed")
            self.pass_msg_to_queue({"display":"table" ,  "msg":self.msg})
            sleep(0.1)
            self.pass_msg_to_queue({"display":"table" ,  "msg":self.msg})
            self.logger.info( self.msg )
            return True , self.msg
        except Exception as e:
            print(e)
            return False , self.msg

def getEncodedString(string):
    string = str(string)
    base64_bytes = base64.b64encode(string.encode("ascii"))
    return base64_bytes.decode("ascii")


class ThreadManager:
    def __init__(self, websocket_manager ,userId , user_queue , load_new_contract_obj , pool):
        self.websocket_manager =  websocket_manager
        self.today = datetime.now().date()
        self.load_new_contract_obj = load_new_contract_obj
        self.master_df = None
        self.pool  = pool
        self.fyers = None
        self.index_df = None
        self.userId = userId
        self.user_queue = user_queue
        self.broker = "fyers"
        self.shoonya = ShoonyaApiPy()
        self.fyers = fyersModel.SessionModel( client_id = "" ,  secret_key = "" ,  redirect_uri = "" ,  response_type = "" ,  grant_type = "" )
        self.angleone = SmartConnect(api_key="")
        self.sws = None
        
        self.nifty_details = {
            "lot": 1,
            "expiry": None,
            "primum_profit_point": 20,
            "max_drawdown": 50000,
            "primum_trail_sl_activation_point": 10,
            "ce_strike_distance": 0,
            "pe_strike_distance": 0,
            "premium_sl_point": 20,
            "timeframe": 1,
            "start_method": "BREAK",
            "increase_sl_method": "HL",
            "tp_type" :"full" ,
            "CE": True,
            "PE": True,
        }
 
        self.banknifty_details = {
            "lot": 1,
            "expiry": None,
            "primum_profit_point": 20,
            "max_drawdown": 50000,
            "primum_trail_sl_activation_point": 10,
            "ce_strike_distance": 0,
            "pe_strike_distance": 0,
            "premium_sl_point": 20,
            "timeframe": 1,
            "start_method": "BREAK",
            "increase_sl_method": "HL",
            "tp_type" :"full" ,
            "CE": True,
            "PE": True,
        }
        self.form_msg =  { "nifty_atm":""  ,  "nifty_ce_ltp":""  , "nifty_pe_ltp":"" , "banknifty_atm":""  ,  "banknifty_ce_ltp":""  , "banknifty_pe_ltp":"" }
        self.booked_nifty_pl = deque([("DEMO", 0), ("LIVE", 0)])
        self.booked_banknifty_pl = deque([("DEMO", 0), ("LIVE", 0)])
        self.locks: Dict[str, async_lock ] = {}
        self.nifty_thread = None
        self.banknifty_thread = None
        self.trading_mode = "DEMO"
        self.real_max_trade = self.demo_max_trade = 5
        self.demo_orders = self.real_orders = []
        self.index_df = None
        # self.nifty_minimum_lot = None
        # self.banknifty_minimum_lot = None
        self.logger = self._setup_user_logger()
        self._setup_background_event_loop()
        Thread(target=self.ce_pe_ltp_feeding).start()

    def _setup_user_logger(self):
        date_folder = self.today.strftime("%Y-%m-%d")
        log_directory = os.path.join("logs", date_folder)
        os.makedirs(log_directory, exist_ok=True)
        logger = logging.getLogger(f"user_{self.userId}")
        logger.setLevel(logging.DEBUG)
        self.log_file_path = os.path.join(log_directory, f"{self.userId}.log")
        file_handler = logging.FileHandler(self.log_file_path)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s" )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger
    
    def read_logs_as_dict(self):
        logs_as_dict = []

        if not os.path.exists(self.log_file_path):
            self.logger.error("Log file not found.")
            return {"error": "Log file not found"}

        try:
            with open(self.log_file_path, "r") as log_file:
                for line in log_file:
                    parts = line.strip().split(" - ")
                    if len(parts) < 4:
                        continue  # Skip lines that don't match the expected format

                    timestamp, logger_name, log_level, message = parts
                    logs_as_dict.append({
                        "timestamp": timestamp,
                        "logger_name": logger_name,
                        "log_level": log_level,
                        "message": message
                    })
            logs_as_dict = logs_as_dict[::-1]

        except Exception as e:
            self.logger.error(f"Error reading log file: {e}")
            return {"error": str(e)}

        return logs_as_dict

    def check_date_and_reset_logger(self):
        current_date = datetime.now().date()
        if current_date != self.today:
            self.today = current_date
            self.log_file_path, self.logger = self._setup_user_logger()
            self.logger.info(f"Date changed. New log file created for {self.today}.")

    def load_order(self , trading_mode , _type , symbol , qty , price , status , _time = None):
        if trading_mode =="DEMO":
            self.demo_orders.insert(0 ,  {"time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")  ,"type":_type , "symbol":symbol , "qty":qty , "price":price , "status":status } )
        elif trading_mode =="LIVE":
            self.real_orders.insert(0 , {"time":_time ,"type":_type , "symbol":symbol , "qty":qty , "price":price , "status":status } )

    def _setup_background_event_loop(self):
        self.background_loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self._run_background_loop, daemon=True)
        self.loop_thread.start()

    def _run_background_loop(self):
        asyncio.set_event_loop(self.background_loop)
        self.background_loop.run_forever()

    def put_in_async_queue(self, item):
        asyncio.run_coroutine_threadsafe(self.user_queue.put(item), self.background_loop)

    def ce_pe_ltp_feeding(self):
        while True:
            sleep(1)
            # print(self.nifty_details)
            if self.master_df is not None:
                if self.nifty_thread is not None:
                    if not self.nifty_thread.is_alive():
                        try:
                            # print(self.nifty_details['expiry'] , type(self.nifty_details['expiry']))
                            # print(self.master_df["expiry"].iloc[25] , type(self.master_df["expiry"].iloc[25]))
                            self.form_msg['nifty_atm'] = round( self.master_df.loc[(self.master_df['underlying_symbol'] == "NIFTY") &  (self.master_df['instrument'] ==  "INDEX" ) , 'ltp'].iloc[0] / 50 ) * 50
                            if self.form_msg['nifty_atm'] == 0 or self.nifty_details['expiry'] is None:
                                continue
                            ce_strike = self.form_msg['nifty_atm'] + self.nifty_details['ce_strike_distance']
                            pe_strike = self.form_msg['nifty_atm'] - self.nifty_details['pe_strike_distance']
                            # print("ce_strike" , ce_strike , type(ce_strike))
                            # print(self.master_df["strike"].iloc[25]  , type(self.master_df["strike"].iloc[25]))
                            # print("form expiry" ,  pd.to_datetime(self.nifty_details['expiry']).date() , type( pd.to_datetime(self.nifty_details['expiry']).date()) )
                            # print("master_df date" , self.master_df['expiry'].iloc[25] , type(self.master_df['expiry'].iloc[25]) )                            
                            self.form_msg['nifty_ce_ltp'] = self.master_df.loc[(self.master_df['underlying_symbol'] == "NIFTY") &  (self.master_df["instrument"] == "OPTIDX" ) &  (self.master_df["strike"] == ce_strike ) & ( self.master_df["option_type"] == "CE" )  & ( self.master_df["expiry"] == pd.to_datetime(self.nifty_details['expiry']).date() ) , 'ltp'].iloc[0]
                            self.form_msg['nifty_pe_ltp'] = self.master_df.loc[(self.master_df['underlying_symbol'] == "NIFTY") &  (self.master_df["instrument"] == "OPTIDX" ) &  (self.master_df["strike"] == pe_strike ) & ( self.master_df["option_type"] == "PE" )  & ( self.master_df["expiry"] == pd.to_datetime(self.nifty_details['expiry']).date() ) , 'ltp'].iloc[0]
                            self.put_in_async_queue({"display":"form" ,  "msg":self.form_msg})
                        except Exception as e:
                            print(e)

 
                if self.banknifty_thread is not None:
                    if not self.banknifty_thread.is_alive():
                        try:
                            self.form_msg['banknifty_atm'] = round( self.master_df.loc[(self.master_df['underlying_symbol'] == "BANKNIFTY") &  (self.master_df['instrument'] ==  "INDEX" ) , 'ltp'].iloc[0] / 100 ) * 100
                            if self.form_msg['banknifty_atm'] == 0 or self.banknifty_details['expiry'] is None:
                                continue
                            ce_strike = self.form_msg['banknifty_atm'] + self.banknifty_details['ce_strike_distance']
                            pe_strike = self.form_msg['banknifty_atm'] - self.banknifty_details['pe_strike_distance']
                            self.form_msg['banknifty_ce_ltp'] = self.master_df.loc[(self.master_df['underlying_symbol'] == "BANKNIFTY") &  (self.master_df["instrument"] == "OPTIDX" ) &  (self.master_df["strike"] == ce_strike ) & ( self.master_df["option_type"] == "CE" ) & ( self.master_df["expiry"] == pd.to_datetime(self.banknifty_details['expiry']).date() ) , 'ltp'].iloc[0]
                            self.form_msg['banknifty_pe_ltp'] = self.master_df.loc[(self.master_df['underlying_symbol'] == "BANKNIFTY") &  (self.master_df["instrument"] == "OPTIDX" ) &  (self.master_df["strike"] == pe_strike ) & ( self.master_df["option_type"] == "PE" ) & ( self.master_df["expiry"] == pd.to_datetime(self.banknifty_details['expiry']).date() ) , 'ltp'].iloc[0]
                            self.put_in_async_queue({"display":"form" ,  "msg":self.form_msg})                    
                        except Exception as e:
                            print(e)
       


    def update_tradingmode_lots(self ,tradingmode , lot):
        if datetime.now().time() > Time(9, 15) and datetime.now().time() < Time(15, 30 ) :
            if lot > self.real_max_trade:
                return False , "you cant increase lot value after 9:15"
        self.trading_mode = tradingmode
        self.real_max_trade = self.demo_max_trade = lot
        return True , "New Value Updated"

    def update_broker(self , broker):
        if ( self.nifty_thread is None or not self.nifty_thread.is_alive()) \
            and ( self.banknifty_thread is  None or not self.banknifty_thread.is_alive() ):
                if not self.server_status():
                    self.broker = broker
                    print(self.broker)
                    return True , "Broker Changed Successfully"
                else:
                    return False , "First Stop Broker"
        return False  , "First Close Running Order Process"

    def get_lock(self, user: str) -> async_lock:
        if user not in self.locks:
            self.locks[user] = async_lock()
        return self.locks[user]

    def get_booked_pl(self):
        def get_value_from_deque(deque_obj, key):
            for k, v in deque_obj:
                if k == key:
                    return v
            return None
        if self.trading_mode == "DEMO":
            return {
                "nifty": get_value_from_deque(self.booked_nifty_pl, "DEMO"),
                "banknifty": get_value_from_deque(self.booked_banknifty_pl, "DEMO"),
                }
        elif self.trading_mode == "LIVE":
            return {
                "nifty": get_value_from_deque(self.booked_nifty_pl, "LIVE"),
                "banknifty": get_value_from_deque(self.booked_banknifty_pl, "LIVE"),
                }


    def get_individual_booked_pl(self , index):
        def get_value_from_deque(deque_obj, key):
            for k, v in deque_obj:
                if k == key:
                    return v
            return None

        if self.trading_mode == "DEMO":
            if index == "NIFTY":
                return  float(get_value_from_deque(self.booked_nifty_pl, "DEMO"))
            else:
                return float(get_value_from_deque(self.booked_banknifty_pl, "DEMO"))

        elif self.trading_mode == "LIVE":
            if index == "NIFTY":
                return  float(get_value_from_deque(self.booked_nifty_pl, "LIVE"))
            else:
                return float(get_value_from_deque(self.booked_banknifty_pl, "LIVE"))

    def update_booked_pl(self ,  index, pl):
        def update_deque(deque_obj, key, pl ):
            for i, (k, v) in enumerate(deque_obj):
                if k == key:
                    deque_obj[i] = (k, v + pl)  # Update the tuple
                    return v + pl
            return None  # Key not found

        if self.trading_mode == "DEMO":
            if index == "NIFTY":
                return update_deque(self.booked_nifty_pl, "DEMO", pl)
            elif index == "BANKNIFTY":
                return update_deque(self.booked_banknifty_pl, "DEMO", pl)
        elif self.trading_mode == "LIVE":
            if index == "NIFTY":
                return update_deque(self.booked_nifty_pl, "LIVE", pl)
            elif index == "BANKNIFTY":
                return update_deque(self.booked_banknifty_pl, "LIVE", pl)

    def get_details(self):
        if self.master_df is not None:
            nifty_expirys = sorted([expiry.strftime("%Y-%m-%d") for expiry in self.master_df[(self.master_df["underlying_symbol"] == "NIFTY") & (self.master_df["instrument"] == 'OPTIDX' )]["expiry"].unique()])
            banknifty_expirys = sorted([expiry.strftime("%Y-%m-%d") for expiry in self.master_df[(self.master_df["underlying_symbol"] == "BANKNIFTY") & (self.master_df["instrument"] == 'OPTIDX')]["expiry"].unique()])
        else:
            nifty_expirys = []
            banknifty_expirys = []
        # print(nifty_expirys)

        if self.nifty_details['expiry']:
            nifty_expiry_string = self.nifty_details['expiry'].strftime("%Y-%m-%d")
        else:
            nifty_expiry_string = self.nifty_details['expiry']

        if self.banknifty_details['expiry']:
            banknifty_expiry_string = self.banknifty_details['expiry'].strftime("%Y-%m-%d")
        else:
            banknifty_expiry_string = self.banknifty_details['expiry']

        return { "nifty_expiry_list":nifty_expirys ,"nifty_expiry":nifty_expiry_string , "nifty_perameter":self.nifty_details  ,"nifty_table_details": self.nifty_thread.msg if self.nifty_thread else None ,
                "banknifty_expiry_list":banknifty_expirys ,"banknifty_expiry":banknifty_expiry_string , "banknifty_perameter":self.banknifty_details  ,"banknifty_table_details": self.banknifty_thread.msg if self.banknifty_thread else None
                }

    def server_status(self):
        print("broker" , self.broker )
        try:
            if self.broker =="shoonya":
                _state = self.shoonya.get_positions()
                return True
            elif self.broker == "fyers":
                a = self.fyers.get_profile()
                if a['s'] == 'ok':
                    return True
                else:
                    return False
            elif self.broker == "angleone":
                a = self.angleone._getRequest("api.user.profile",{"refreshToken": self.angleone.refresh_token })
                if a['status'] == True and a['message'] == 'SUCCESS':
                    return True
                return False          
            return False
        except Exception as e:
            print(e)
            return False



        
    def stop_broker(self):
        print("broker" , self.broker )
        try:
            if self.broker =="shoonya":
                try:
                    self.shoonya_close_websocket()
                except Exception as e:
                    print("closing shoonya:" , e)
                response = self.shoonya.logout()
                print(response)
                print(type(response))
                if response['stat'] == 'Ok':
                    self.shoonya = ShoonyaApiPy()
                    return True 
 
            elif self.broker =="angleone":
                try:
                    self.sws.close_connection()
                except Exception as e:
                    print("closing fyers:" , e)
                response = self.angleone._postRequest("api.logout",{"clientcode": self.angleone.userId })
                print(response)

                if response['status']  or  (not self.server_status()):
                    self.angleone = SmartConnect(api_key="")
                    return True 

            elif self.broker == "fyers":
                try:
                    self.fyers_stop_web_socket()
                    self.fyers = fyersModel.SessionModel( client_id = "" ,  secret_key = "" ,  redirect_uri = "" ,  response_type = "" ,  grant_type = "" )
                    return True
                except Exception as e:
                    print("closing fyers:" , e)

            return False
        except Exception as e:
            print(e)
        
########################################################

    def connect_shoonya(self):
        
        self.today = datetime.now().date()
        self.check_date_and_reset_logger()
        self.shoonya = ShoonyaApiPy()

        user_config =  fetch_credentials_by_userId(self.pool , self.userId ,"shoonya" )
        print(user_config)

        if user_config is None:
            return False
        try:
            otp = pyotp.TOTP(user_config['shoonya_twofa']).now()
            self.ret = self.shoonya.login(userid=user_config['shoonya_user_id'] , password= user_config['shoonya_password'] , twoFA=otp, vendor_code=user_config['shoonya_vendor_code'], api_secret=user_config['shoonya_api_key'], imei=user_config['shoonya_imei'] )
            print("self.ret")
            print(self.ret)
            if self.ret['stat'] != "Ok":
                return False
        except Exception as e:
            print(e)
            return False
        else:
            try:
                index_df , option_df , future_df  = self.load_new_contract_obj.get_df()
                if index_df is None or  option_df is None or future_df is None:
                    return False
 
                nse_instrument_df = pd.read_csv("https://api.shoonya.com/NSE_symbols.txt.zip")
                nse_instrument_df = nse_instrument_df[(nse_instrument_df['Instrument']=="INDEX") & (nse_instrument_df['TradingSymbol'].isin( ["NIFTY INDEX" , "NIFTY BANK"] ))]
                nse_instrument_df = nse_instrument_df.drop('Unnamed: 7' , axis=True)

                index_df['token'] = 0
                # index_df['token'] = index_df['token'].astype(int)
                index_df['trading_symbol'] = ""
                
                index_df.loc[0, 'token'] = nse_instrument_df.loc[nse_instrument_df['Symbol'] ==  "Nifty 50" , 'Token'].iloc[0]
                index_df.loc[0, 'trading_symbol'] = nse_instrument_df.loc[nse_instrument_df['Symbol'] == "Nifty 50" , 'Symbol'].iloc[0]
                index_df.loc[1, 'token'] = nse_instrument_df.loc[nse_instrument_df['Symbol'] ==  "Nifty Bank" , 'Token'].iloc[0]
                index_df.loc[1, 'trading_symbol'] = nse_instrument_df.loc[nse_instrument_df['Symbol'] == "Nifty Bank" , 'Symbol'].iloc[0]


                nfo_instrument_df = pd.read_csv("https://api.shoonya.com/NFO_symbols.txt.zip")
                nfo_instrument_df = nfo_instrument_df.drop('Unnamed: 10' , axis=True)
                nfo_instrument_df['Expiry'] = pd.to_datetime(nfo_instrument_df['Expiry'], errors='coerce').dt.date
 
                future_instrument_df = nfo_instrument_df[(nfo_instrument_df['Instrument'] == "FUTIDX") & (nfo_instrument_df['Symbol'].isin(['NIFTY' , 'BANKNIFTY']) )  ]
                future_instrument_df = future_instrument_df[future_instrument_df['Expiry'] == future_instrument_df['Expiry'].min() ]
                future_df = future_df.merge( future_instrument_df[['Token', 'TradingSymbol', 'Symbol']], left_on=['underlying_symbol'], right_on=['Symbol'], how='left' )
                future_df.drop(columns=['Symbol'], inplace=True)
                nifty_price = self.shoonya.get_quotes(exchange="NFO", token= str(future_df[future_df['underlying_symbol']=="NIFTY"]["Token"].iloc[0]) )['lp']
                sleep(0.3)
                banknifty_price = self.shoonya.get_quotes(exchange="NFO", token= str(future_df[future_df['underlying_symbol']=="BANKNIFTY"]["Token"].iloc[0]) )['lp']

                nfo_instrument_df = nfo_instrument_df[(nfo_instrument_df['Instrument'] == "OPTIDX") & nfo_instrument_df['Symbol'].isin(["NIFTY" , "BANKNIFTY"]) ]
                nfo_instrument_df["Expiry"] = pd.to_datetime(nfo_instrument_df["Expiry"])  # Convert expiry to datetime
                option_df['expiry'] = pd.to_datetime(option_df['expiry'])
                
 
                nfo_instrument_df = option_df.merge(
                    nfo_instrument_df[['Token', 'TradingSymbol', 'Expiry', 'Symbol', 'OptionType', 'StrikePrice']],
                    left_on=['expiry', 'underlying_symbol', 'option_type', 'strike'],
                    right_on=['Expiry', 'Symbol', 'OptionType', 'StrikePrice'],
                    how='left'
                )

                nfo_instrument_df.drop(columns=['Expiry', 'Symbol', 'OptionType', 'StrikePrice'], inplace=True)
    
                nifty_instrument_df = nfo_instrument_df[nfo_instrument_df['underlying_symbol'] == "NIFTY"]
                nifty_atm  = int(round(float(nifty_price) / 50) * 50)
                nifty_instrument_df = nifty_instrument_df[ (nifty_atm-2000 < nifty_instrument_df['strike']) & (nifty_instrument_df['strike'] < nifty_atm + 2000)]

                banknifty_instrument_df = nfo_instrument_df[nfo_instrument_df['underlying_symbol'] == "BANKNIFTY"]
                banknifty_atm  = int(round(float(banknifty_price) /100) * 100)
                banknifty_instrument_df = banknifty_instrument_df[ (banknifty_atm-4000 < banknifty_instrument_df['strike']) & ( banknifty_instrument_df['strike'] < banknifty_atm + 4000)]

                df = pd.concat([  future_df, nifty_instrument_df , banknifty_instrument_df ], ignore_index=True)
                df.rename(columns={ "Token": "token" , "TradingSymbol": "trading_symbol" }, inplace=True)
                df = pd.concat([ index_df, df ], ignore_index=True)
                df["expiry"] = df["expiry"].dt.date
                df['ltp'] = 0.0
                df['token'] = df['token'].astype(int)
                df['token'] = df['token'].astype(str)
                df['strike'] = df['strike'].astype(float)
                self.master_df = df
                print(self.master_df)
                return True
            except Exception as e:
                print(e)
                return False
           
    def shoonya_connect_to_websocket(self):
        def event_handler_order_update(message):
            # print("order event: " + str(message))
            pass

        def setal_tick(tick):
            if 'lp' in tick:
                # print(tick)
                # print("------------------------")
                # print(self.master_df.loc[self.master_df['token'] == tick['tk']].loc[0] , type(self.master_df.loc[self.master_df['token'] == tick['tk']].loc[0]) )
                
                
                self.master_df.loc[self.master_df['token'] == tick['tk'] , 'ltp'] = float(tick["lp"])
 
        def event_handler_quote_update(message):
            # print(message)
            setal_tick(message)
            
        def open_callback():
            global socket_opened
            socket_opened = True
            token= []
            print("token" , token)
            for index , row in self.master_df.iterrows():
                if row['instrument']=="INDEX":
                    token.append(f"NSE|{row['token']}")
                else:
                    token.append(f"NFO|{row['token']}")
            self.shoonya.subscribe( token, feed_type='t')
            print("token" , token)
            print('app is connected')
            #api.subscribe(['NSE|22', 'BSE|522032'])
 
        ret = self.shoonya.start_websocket(order_update_callback=event_handler_order_update, subscribe_callback=event_handler_quote_update, socket_open_callback=open_callback )
        # print(vars(self.shoonya))
        
    def shoonya_close_websocket(self):
        state = self.shoonya.close_websocket()
                    
########################################################

    def get_fyers_login_url(self, user):
        self.today = datetime.now().date()
        self.check_date_and_reset_logger()
        try:
            data = fetch_credentials_by_userId(self.pool , user , 'fyers')
            print("credentials" , data)
            if data["fyers_apiKey"] == "":
                raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"User data Not Found" )
        except Exception as e:
            raise HTTPException( status_code=status.HTTP_400_BAD_REQUEST, detail=f"User data Not Found" )
        try:
            self.fyers = fyersModel.SessionModel(
                client_id=data["fyers_apiKey"],
                secret_key=data["fyers_secretKey"],
                redirect_uri=data["fyers_redirectUrl"],
                response_type=data["fyers_responseType"],
                grant_type=data["fyers_grantType"],
            )
            print("okokok")
            generateTokenUrl = self.fyers.generate_authcode()
            print("okokok")
            print(generateTokenUrl)
            return True ,  generateTokenUrl
        except Exception as e:
            print(f"Error during token generation: {e}")
            return False ,  None

    def fyers_load_access_token(self , url , userId):
        access_token = None
        try:
            data = fetch_credentials_by_userId(self.pool , userId , 'fyers')     
            parsed = urlparse(url)
            print("parsed" , parsed)
            auth_code = parse_qs(parsed.query)["auth_code"][0]
            print("auth_code" , auth_code)
            self.fyers.set_token(auth_code)
            response = self.fyers.generate_token()
            print("response" ,response)
            access_token = response["access_token"]

            if access_token:         
                self.fyers = fyersModel.FyersModel(
                    client_id=data["fyers_apiKey"],
                    is_async=False,
                    token=access_token,
                    log_path=os.getcwd() )
            else:
                print("Access token is empty.")
                return False

        except Exception as e:
            print(f"Error during token generation: {e}")
            return False
        else:
            try: 
                index_df , option_df , future_df  = self.load_new_contract_obj.get_df()
                if index_df is None or  option_df is None or future_df is None:
                    return False
 
                df = pd.read_csv( "https://public.fyers.in/sym_details/NSE_FO.csv", header=None )
                df.columns = [ "Fytoken", "SymbolDetails", "ExchangeInstrumentType", "MinimumLotSize", "TickSize", "ISIN", "TradingSession", "LastUpdateDate", "ExpiryDate", "SymbolTicker", "Exchange", "Segment", "ScripCode", "UnderlyingSymbol", "UnderlyingScripCode", "StrikePrice", "OptionType", "UnderlyingFyToken", "ReservedColumnString", "ReservedColumnInt", "ReservedColumnFloat" ]
                df = df[ df["UnderlyingSymbol"].isin(["NIFTY", "BANKNIFTY"])]
                df["ExpiryDate"] = pd.to_datetime( df["ExpiryDate"], unit="s", utc=True )
                df["ExpiryDate"] = df["ExpiryDate"].dt.tz_convert( "Asia/Kolkata" )
                df["ExpiryDate"] = df["ExpiryDate"].dt.tz_localize(None)
                df["ExpiryDate"] = df["ExpiryDate"].dt.date
  
                fyers_future_df = df[df['ExchangeInstrumentType']==11]
                fyers_future_df = fyers_future_df.loc[fyers_future_df.groupby("UnderlyingSymbol")["ExpiryDate"].idxmin()]
                future_df = future_df.merge(
                            fyers_future_df[['UnderlyingSymbol', 'Fytoken', 'SymbolTicker']], 
                            left_on='underlying_symbol', 
                            right_on='UnderlyingSymbol', 
                            how='left'
                        ).drop(columns=['UnderlyingSymbol'])
                future_df.rename(columns={ "Fytoken": "token" , "SymbolTicker": "trading_symbol" }, inplace=True)
                future_df['expiry'] = future_df['expiry'].dt.date
  
                nifty_future_price = self.fyers.quotes( {"symbols": future_df[future_df["underlying_symbol"]=="NIFTY"]["trading_symbol"].iloc[0] } )["d"][0]["v"]["lp"]
                sleep(0.5)
                banknifty_future_price = self.fyers.quotes( {"symbols": future_df[future_df["underlying_symbol"]=="BANKNIFTY"]["trading_symbol"].iloc[0] } )["d"][0]["v"]["lp"]

 
                nifty_df = df[ (df["UnderlyingSymbol"] == "NIFTY") & (df["ExchangeInstrumentType"] ==14 )]
                nifty_smallest_dates = nifty_df['ExpiryDate'].sort_values().unique()[:3]
                nifty_df = nifty_df[nifty_df['ExpiryDate'].isin(nifty_smallest_dates)]
                nifty_df = nifty_df.sort_values(by='StrikePrice', ascending=True)
                nifty_df = nifty_df.reset_index(drop=True)
                atm_strike_idx = (nifty_df['StrikePrice'] - nifty_future_price ).abs().idxmin()
                nifty_df = nifty_df [(nifty_df.index >= (atm_strike_idx - 200 )) & (nifty_df.index <= (atm_strike_idx + 200 ))  ]

                banknifty_df = df[ (df["UnderlyingSymbol"] == "BANKNIFTY")  & (df["ExchangeInstrumentType"] ==14 ) ]
                banknifty_smallest_dates = banknifty_df['ExpiryDate'].sort_values().unique()[:2]
                banknifty_df = banknifty_df[banknifty_df['ExpiryDate'].isin(banknifty_smallest_dates)]
                banknifty_df = banknifty_df.sort_values(by='StrikePrice', ascending=True)
                banknifty_df = banknifty_df.reset_index(drop=True)
                atm_strike_idx = (banknifty_df['StrikePrice'] - banknifty_future_price ).abs().idxmin()
                banknifty_df = banknifty_df [(banknifty_df.index >= (atm_strike_idx - 150 )) & (banknifty_df.index <= (atm_strike_idx + 150 ))  ]

                fyers_option_df = pd.concat( [ nifty_df, banknifty_df ], ignore_index=True )
  
                fyers_option_df = fyers_option_df[[ 'Fytoken', 'MinimumLotSize',  'ExpiryDate' ,'SymbolTicker','UnderlyingSymbol' ,  'StrikePrice' , 'OptionType'  ]]
                fyers_option_df['ExpiryDate'] = pd.to_datetime( fyers_option_df['ExpiryDate'])
                
                option_df['expiry'] = option_df['expiry'].dt.date
                fyers_option_df['ExpiryDate'] = fyers_option_df['ExpiryDate'].dt.date

                option_df['strike'] = option_df['strike'].round(2)
                fyers_option_df['StrikePrice'] = fyers_option_df['StrikePrice'].round(2)

                option_df['option_type'] = option_df['option_type'].str.upper()
                fyers_option_df['OptionType'] = fyers_option_df['OptionType'].str.upper()

                merged_option_df = option_df.merge(
                    fyers_option_df[['UnderlyingSymbol', 'ExpiryDate', 'OptionType', 'StrikePrice', 'Fytoken', 'SymbolTicker']], 
                    left_on=['underlying_symbol', 'expiry', 'option_type', 'strike'], 
                    right_on=['UnderlyingSymbol', 'ExpiryDate', 'OptionType', 'StrikePrice'], 
                    how='left'
                    ).drop(columns=['UnderlyingSymbol', 'ExpiryDate', 'OptionType', 'StrikePrice'])
                merged_option_df = merged_option_df.dropna(subset=['Fytoken', 'SymbolTicker'])
                merged_option_df.rename(columns={ "Fytoken": "token" , "SymbolTicker": "trading_symbol" }, inplace=True)

                self.master_df = pd.concat( [ future_df , merged_option_df], ignore_index=True )
                self.master_df['ltp'] = 0.0
                print("self.master_df")
                print(self.master_df)
            except Exception as e:
                print(e)
                return False
        sleep(0.5)
        return self.server_status()

    def fyers_run_web_socket(self):

        if not self.server_status():
            return False

        def onmessage(message):
            # global self.option_df
            try:
                # print(message["ltp"])
                self.master_df.loc[self.master_df['trading_symbol'] == message['symbol'] , 'ltp'] = message["ltp"]
 
            except Exception as e:
                print(e)
                print(message)

        def onerror(message):
            print("Error:", message)
            self.put_in_async_queue({"diplay":"notification" , "title":"NIFTY" , "msg":f"webscoket Error : {message}"})           

        def onclose(message):
            self.ws_fyers.close_connection()
#            self.put_in_async_queue({"diplay":"log" , "title":"NIFTY" , "msg":f"websocket close : {message}"})
 #           self.put_in_async_queue({"diplay":"notification" , "title":"NIFTY" , "msg":f"websocket close : {message}"})            
            print("Connection closed:", message)

        def onopen():
            # self.put_in_async_queue({"diplay":"notification" , "title":"WebSocket" , "msg":"Connecting to websocket"})           
            symbols = self.master_df["trading_symbol"].to_list()
            # print(symbols)
            self.ws_fyers.subscribe(symbols=symbols, data_type="SymbolUpdate")
            self.ws_fyers.keep_running()

        self.ws_fyers = data_ws.FyersDataSocket(
            access_token=self.fyers.token,  # Access token in the format "appid:accesstoken"
            log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
            litemode=False,  # Lite mode disabled. Set to True if you want a lite response.
            write_to_file=False,  # Save response in a log file instead of printing it.
            reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
            on_connect=onopen,  # Callback function to subscribe to data upon connection.
            on_close=onclose,  # Callback function to handle WebSocket connection close events.
            on_error=onerror,  # Callback function to handle WebSocket errors.
            on_message=onmessage,  # Callback function to handle incoming messages from the WebSocket.
        )

        def run_websocket():
            self.ws_fyers.connect()
            self.ws_fyers.keep_running

        websocket_thread = Thread(target=run_websocket)
        websocket_thread.start()
        return True

    def fyers_stop_web_socket(self):
        try:
            if self.ws_fyers:
                if self.ws_fyers.is_connected():
                    self.ws_fyers.__ws_run = False
                    # self.ws_fyers.keep_running = False
                print(self.ws_fyers.OnClose)  
                # self.put_in_async_queue({"display": "notification", "title": "WebSocket", "msg": "WebSocket stopped successfully."})
                print("WebSocket stopped successfully.")
            else:
                print("WebSocket is not running.")
                self.put_in_async_queue({"display": "notification", "title": "WebSocket", "msg": "WebSocket is not running."})
            return True
        except Exception as e:
            print(f"Error stopping WebSocket: {e}")
            self.put_in_async_queue({"display": "notification", "title": "WebSocket", "msg": f"Error stopping WebSocket: {e}"})
            return False


    def connect_angleone(self):        
        self.today = datetime.now().date()
        self.check_date_and_reset_logger()
        user_config =  fetch_credentials_by_userId(self.pool , self.userId ,"angleone" )
        print(user_config)

        if user_config is None:
            return False
        try:
            self.angleone =SmartConnect(api_key= user_config['angle_api_key'] )
            print(self.angleone)
            data = self.angleone.generateSession( user_config['angle_user_id'] ,user_config['angle_pin'], pyotp.TOTP(user_config['angle_totp']).now() ) 
            
            if (data['status']== True) and (data['message']=="SUCCESS"):
                authtoken = data['data']['jwtToken']
                refreshToken = data['data']['refreshToken']
                feedToken=self.angleone.getfeedToken()
                print("feedToken" , feedToken)
                self.angleone.refresh_token = refreshToken
                self.userId = user_config['angle_user_id']

                self.sws = SmartWebSocketV2(authtoken, user_config['angle_api_key'] , user_config['angle_user_id'] , feedToken ,  retry_delay=3 )
            else:
                return False
        except Exception as e:
            print(e)
            return False
        else:
            try:
                index_df , option_df , future_df  = self.load_new_contract_obj.get_df()
                if index_df is None or  option_df is None or future_df is None:
                    return False
 
                response = requests.get("https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json")

                if response.status_code == 200:
                    df = pd.DataFrame(response.json())
                else:
                    return False

                df = df[(df['exch_seg']=='NFO') & (df['instrumenttype'].isin(['OPTIDX', 'FUTIDX'])) & (df['name'].isin([ 'BANKNIFTY', 'NIFTY']))]
                df['expiry'] = pd.to_datetime(df['expiry'], format='%d%b%Y')
                df['strike'] = df['strike'].astype('float64')
                # df['token'] = pd.to_numeric(df['token'], errors='coerce').astype('Int64')

                future_angle_df = df[df['instrumenttype'] == 'FUTIDX']
                future_angle_df = future_angle_df[future_angle_df['expiry'] == future_angle_df['expiry'].min()]
                                
                future_df = future_df.merge( future_angle_df[['token', 'symbol', 'name']],  left_on='underlying_symbol',  right_on='name',  how='left' ).drop(columns=['name'])

                nifty_price = self.angleone.ltpData(exchange="NFO" ,tradingsymbol= str(future_df[future_df['underlying_symbol']=="NIFTY"]["symbol"].iloc[0]) ,
                                                    symboltoken= future_df[future_df['underlying_symbol']=="NIFTY"]["token"].iloc[0] )
                print("nifty_price" ,nifty_price)
                banknifty_price = self.angleone.ltpData(exchange="NFO" ,tradingsymbol= str(future_df[future_df['underlying_symbol']=="BANKNIFTY"]["symbol"].iloc[0]) ,
                                                    symboltoken= future_df[future_df['underlying_symbol']=="BANKNIFTY"]["token"].iloc[0] )
                print("banknifty_price" , banknifty_price)


                option_angle_df = df[df['instrumenttype'] == 'OPTIDX']
                option_angle_df['strike'] = pd.to_numeric( option_angle_df['strike'], errors='coerce').astype('Int64')

                nifty_angle_df = option_angle_df[ option_angle_df['name'] == "NIFTY" ]
                print("---------")
                nifty_atm   = int(round(float(nifty_price['data']['ltp'] ) / 50) * 50)
                nifty_angle_df['strike'] = nifty_angle_df['strike']/100
                nifty_angle_df = nifty_angle_df[ (nifty_atm-2000 < nifty_angle_df['strike']) & (nifty_angle_df['strike'] < nifty_atm + 2000)]
                nifty_angle_df = nifty_angle_df[ nifty_angle_df['expiry'].isin(nifty_angle_df['expiry'].sort_values().unique()[:3]) ]

                banknifty_angle_df = option_angle_df[ option_angle_df['name'] == "BANKNIFTY" ]
                banknifty_atm  = int(round(float(banknifty_price['data']['ltp']) /100) * 100)
                banknifty_angle_df['strike'] = banknifty_angle_df['strike']/100
                banknifty_angle_df = banknifty_angle_df[(banknifty_atm-4000 < banknifty_angle_df['strike']) & ( banknifty_angle_df['strike'] < banknifty_atm + 4000 )]
                banknifty_angle_df = banknifty_angle_df[ banknifty_angle_df['expiry'].isin(banknifty_angle_df['expiry'].sort_values().unique()[:2]) ]
                option_angle_df = pd.concat( [ banknifty_angle_df , nifty_angle_df  ] , ignore_index = True )
                option_angle_df['option_type'] =  option_angle_df['symbol'].str.extract(r'(CE|PE)$')
                # option_df['expiry'] = pd.to_datetime(option_df['expiry']).dt.date
                # option_angle_df['expiry'] = pd.to_datetime(option_angle_df['expiry']).dt.date

                option_df = option_df.merge(
                    option_angle_df[['name', 'expiry', 'option_type', 'strike', 'token', 'symbol']], 
                    left_on=['underlying_symbol', 'expiry', 'option_type', 'strike'], 
                    right_on=['name', 'expiry', 'option_type', 'strike'], 
                    how='left'
                    ).drop(columns=['name'])
                option_df = option_df.dropna(subset=['token', 'symbol'])

                df  = pd.concat([future_df, option_df ], ignore_index=True)
                df["expiry"] = pd.to_datetime(df["expiry"], errors='coerce')  # Convert to datetime
                df["expiry"] = df["expiry"].dt.date 
                df['ltp'] = 0.0
                # df['token'] = df['token'].astype(str)
                self.master_df = df
                print(self.master_df)
                return True
            except Exception as e:
                print(e)
                return False

    def angle_run_web_socket(self):
        action = 1
        mode = 1
        correlation_id = "abc123"
        token_list = [ { "exchangeType": 2, "tokens":self.master_df['token'].to_list() } ]
        
        def on_data(wsapp, message):
            self.master_df.loc[self.master_df['token'] == message['token'] , 'last_traded_price'] = round(message['last_traded_price']/100,2)
            # print("Ticks: {}".format(message))

        def on_open(wsapp):
            print("on open")
            self.sws.subscribe(correlation_id, mode, token_list)

        def on_error(wsapp, error):
            print(error)

        def on_close(wsapp):
            print("Close")

        def close_connection():
            self.sws.close_connection()

        self.sws.on_open = on_open
        self.sws.on_data = on_data
        self.sws.on_error = on_error
        self.sws.on_close = on_close

        def run_ticksocket(sws):
            sws.connect()

        tick_thread = Thread(target=run_ticksocket, args=(self.sws,))
        tick_thread.start()




    def update_details( self, index ,  lot, expiry, primum_profit_point, max_drawdown, primum_trail_sl_activation_point, CEstrikeDistance , PEstrikeDistance, premium_sl_point, CE , PE , timeframe):
        # self.logger.info(f"{index} Data Saving Process")
        # try:        
            if self.server_status():
                if not CE and not PE:
                    return False, "CE and PE , anyone require Checked"

                if index == "NIFTY":
                    if self.nifty_thread is not None and self.nifty_thread.is_alive():
                        if self.nifty_details['lot'] != lot:
                            return   False,    f"you cant change LOT SIZE in running trade , keep it { self.nifty_details['lot'] }" 
                        elif self.nifty_details['expiry'] != expiry:
                            return  False,    f"you cant change EXPIRY in running trade , keep it { self.nifty_details['expiry'] }"
                        elif self.nifty_details['ce_strike_distance'] != CEstrikeDistance:
                            return  False,    f"you cant change STRIKE DISTANCE in running trade , keep it {self.nifty_details['ce_strike_distance'] }"  
                        elif self.nifty_details['pe_strike_distance'] != PEstrikeDistance:
                            return  False,    f"you cant change STRIKE DISTANCE in running trade , keep it {self.nifty_details['pe_strike_distance'] }"  
                        elif self.nifty_details['CE']  != CE :
                            return  False,    f"you cant change CE ALLOW in running trade , keep it {self.nifty_details['CE'] }" 
                        elif self.nifty_details['PE']  != PE :
                            return  False,    f"you cant change PE ALLOW in running trade , keep it {self.nifty_details['PE'] }" 
                        elif self.nifty_details['timeframe']  != timeframe:
                            return  False,    f"you cant change TIMEFRAME in running trade , keep it {self.nifty_details['timeframe'] }" 
                        else:
                            self.nifty_details['primum_profit_point'] = primum_profit_point
                            self.nifty_details['max_drawdown'] = max_drawdown
                            self.nifty_details['primum_trail_sl_activation_point'] = primum_trail_sl_activation_point
                            self.nifty_details['premium_sl_point'] = premium_sl_point

                            self.nifty_thread.update_values( primum_profit_point ,max_drawdown , primum_trail_sl_activation_point , premium_sl_point )
                            self.nifty_thread.primum_profit_point = primum_profit_point
                            self.nifty_thread.max_drawdown = max_drawdown
                            self.nifty_thread.primum_trail_sl_activation_point = primum_trail_sl_activation_point
                            self.nifty_thread.msg['premium_sl_point'] = premium_sl_point
                            return True, "Updated Successfully"

                    self.nifty_details['lot'] = lot
                    self.nifty_details['expiry'] = expiry
                    self.nifty_details['primum_profit_point'] = primum_profit_point
                    self.nifty_details['max_drawdown'] = max_drawdown
                    self.nifty_details['primum_trail_sl_activation_point'] = primum_trail_sl_activation_point
                    self.nifty_details['ce_strike_distance'] = CEstrikeDistance
                    self.nifty_details['pe_strike_distance'] = PEstrikeDistance
                    self.nifty_details['premium_sl_point'] = premium_sl_point
                    self.nifty_details['CE'] = CE
                    self.nifty_details['PE'] = PE
                    self.nifty_details['timeframe'] = timeframe
                    self.nifty_thread = FyersRunner(self.broker , self.userId, "NIFTY" , self.put_in_async_queue , self.nifty_details['lot']  , self.nifty_details['expiry']  , self.nifty_details['primum_profit_point']  ,
                                                        self.nifty_details['max_drawdown']  , self.nifty_details['primum_trail_sl_activation_point']  , self.nifty_details['ce_strike_distance'] , self.nifty_details['pe_strike_distance']  , self.nifty_details['premium_sl_point']  ,
                                                        self.nifty_details['CE'] , self.nifty_details['PE'] , self.nifty_details['timeframe']  , self.nifty_details['start_method'] ,
                                                        self.nifty_details['increase_sl_method']  , self.nifty_details['premium_sl_point']  ,  self.nifty_details['tp_type'] ,
                                                        self.master_df , self.shoonya , self.fyers ,  self.angleone , self.get_individual_booked_pl("NIFTY")   ,self.update_booked_pl  , self.load_order , self.logger)

                elif index == "BANKNIFTY":
                    if self.banknifty_thread is not None and self.banknifty_thread.is_alive():
                        if self.banknifty_details['lot'] != lot:
                            return   False,    f"you cant change LOT SIZE in running trade , keep it { self.banknifty_details['lot'] }" 
                        elif self.banknifty_details['expiry'] != expiry:
                            return  False,    f"you cant change EXPIRY in running trade , keep it { self.banknifty_details['expiry'] }"
                        elif self.banknifty_details['ce_strike_distance'] != CEstrikeDistance:
                            return  False,    f"you cant change STRIKE DISTANCE in running trade , keep it {self.banknifty_details['ce_strike_distance'] }"  
                        elif self.banknifty_details['pe_strike_distance'] != PEstrikeDistance:
                            return  False,    f"you cant change STRIKE DISTANCE in running trade , keep it {self.banknifty_details['pe_strike_distance'] }"  
                        elif self.banknifty_details['CE']  != CE :
                            return  False,    f"you cant change CE ALLOW in running trade , keep it {self.banknifty_details['CE'] }" 
                        elif self.banknifty_details['PE']  != PE :
                            return  False,    f"you cant change PE ALLOW in running trade , keep it {self.banknifty_details['PE'] }" 
                        elif self.banknifty_details['timeframe']  != timeframe:
                            return  False,    f"you cant change TIMEFRAME in running trade , keep it {self.banknifty_details['timeframe'] }" 
                        else:
                            self.banknifty_details['primum_profit_point'] = primum_profit_point
                            self.banknifty_details['max_drawdown'] = max_drawdown
                            self.banknifty_details['primum_trail_sl_activation_point'] = primum_trail_sl_activation_point
                            self.banknifty_details['premium_sl_point'] = premium_sl_point

                            self.banknifty_thread.update_values( primum_profit_point ,max_drawdown , primum_trail_sl_activation_point , premium_sl_point )
                            self.banknifty_thread.primum_profit_point = primum_profit_point
                            self.banknifty_thread.max_drawdown = max_drawdown
                            self.banknifty_thread.primum_trail_sl_activation_point = primum_trail_sl_activation_point
                            self.banknifty_thread.msg['premium_sl_point'] = premium_sl_point
                            return True, "Updated Successfully"

                    self.banknifty_details['lot'] = lot
                    self.banknifty_details['expiry'] = expiry
                    self.banknifty_details['primum_profit_point'] = primum_profit_point
                    self.banknifty_details['max_drawdown'] = max_drawdown
                    self.banknifty_details['primum_trail_sl_activation_point'] = primum_trail_sl_activation_point
                    self.banknifty_details['ce_strike_distance'] = CEstrikeDistance
                    self.banknifty_details['pe_strike_distance'] = PEstrikeDistance
                    self.banknifty_details['premium_sl_point'] = premium_sl_point
                    self.banknifty_details['CE'] = CE
                    self.banknifty_details['PE'] = PE
                    self.banknifty_details['timeframe'] = timeframe
                    self.banknifty_thread = FyersRunner(self.broker , self.userId , "BANKNIFTY" , self.put_in_async_queue , self.banknifty_details['lot']  , self.banknifty_details['expiry']  , self.banknifty_details['primum_profit_point']  ,
                                                        self.banknifty_details['max_drawdown']  , self.banknifty_details['primum_trail_sl_activation_point']  , self.banknifty_details['ce_strike_distance'] , self.banknifty_details['pe_strike_distance']  , self.banknifty_details['premium_sl_point']  ,
                                                        self.banknifty_details['CE'] , self.banknifty_details['PE'] , self.banknifty_details['timeframe']  , self.banknifty_details['start_method'] ,
                                                        self.banknifty_details['increase_sl_method']  , self.banknifty_details['premium_sl_point']  ,  self.banknifty_details['tp_type'] ,
                                                        self.master_df , self.shoonya , self.fyers ,  self.angleone ,  self.get_individual_booked_pl("BANKNIFTY")   ,self.update_booked_pl  , self.load_order , self.logger )
                return True, "clear OldData"
            else:
                return False, "Not connected to SERVER"

        # except Exception as e:
        #     self.logger.warning(f"Error in {index} update_details ")           
        #     self.logger.warning(e)
        #     return False, "Somthing Wrong"  

    def run_my_thread( self , index ,  break_market ):
        if datetime.now().time() <  Time(9, 16 , 10 ) and Time(15, 14 , 10 ) < datetime.now().time():
            return  False, "Before 9:16 and After 3:15 PM , new order Not Allowed"
        self.logger.info(f"{ index} Thread Starting")
        try:
            if index == "NIFTY":
                if  not ((self.nifty_details['lot'] > 0) and (self.nifty_details['expiry'] is not None) and (self.nifty_details['expiry'] >= datetime.now().date()) and self.nifty_details['max_drawdown'] and self.nifty_details['primum_trail_sl_activation_point'] >=0 and (self.nifty_details['CE'] or self.nifty_details['PE'])):
                    return False, "parameter is not set"
                elif ( self.nifty_details['CE'] and self.nifty_details['PE'] ) and break_market == "MARKET":
                    return  False, "In Market Method , CE and PE Both Now Allow, Choose CE or PE , Not Both"
                if self.nifty_thread is None or self.nifty_thread.finished:
                    self.nifty_thread = FyersRunner(self.broker , self.userId, "NIFTY" , self.put_in_async_queue , self.nifty_details['lot']  , self.nifty_details['expiry']  , self.nifty_details['primum_profit_point']  ,
                                                        self.nifty_details['max_drawdown']  , self.nifty_details['primum_trail_sl_activation_point']  , self.nifty_details['ce_strike_distance'] , self.nifty_details['pe_strike_distance']  , self.nifty_details['premium_sl_point']  ,
                                                        self.nifty_details['CE'] , self.nifty_details['PE'] , self.nifty_details['timeframe']  , self.nifty_details['start_method'] ,
                                                        self.nifty_details['increase_sl_method']  , self.nifty_details['premium_sl_point']  ,  self.nifty_details['tp_type'] ,
                                                        self.master_df , self.shoonya , self.fyers ,  self.angleone ,  self.get_individual_booked_pl("NIFTY") , self.update_booked_pl , self.load_order , self.logger )
                if not self.nifty_thread.is_alive():
                    # print(f"thred is not define or not alive " , self.nifty_thread.is_alive())
                    if not self.nifty_thread.finished:
                        self.nifty_thread.tradingmode = self.trading_mode
                        self.nifty_thread.msg["Entry Type"] = break_market
                        self.nifty_thread.start()
                        return True, "Order Process Start"
                    else:
                        return  False, "First Save Nifty Peramter Again Then Process Will be Start After Pressing START Button"
                else:
                    return False, "Order Process Already Running"

            elif index == "BANKNIFTY":
                if  not ((self.banknifty_details['lot'] > 0) and (self.banknifty_details['expiry'] is not None) and (self.banknifty_details['expiry'] >= datetime.now().date()) and self.banknifty_details['max_drawdown'] and self.banknifty_details['primum_trail_sl_activation_point'] >=0 and (self.banknifty_details['CE'] or self.banknifty_details['PE'])):
                    return False, "parameter is not set"
                
                elif (self.banknifty_details['CE'] and self.banknifty_details['PE']) and break_market == "MARKET":
                    return  False, "In Market Method , CE and PE Both Now Allow, Choose CE or PE , Not Both"
                
                if self.banknifty_thread is None or self.banknifty_thread.finished:
                    self.banknifty_thread = FyersRunner(self.broker , self.userId, "BANKNIFTY" , self.put_in_async_queue , self.banknifty_details['lot']  , self.banknifty_details['expiry']  , self.banknifty_details['primum_profit_point']  ,
                                                        self.banknifty_details['max_drawdown']  , self.banknifty_details['primum_trail_sl_activation_point']  , self.banknifty_details['ce_strike_distance'] , self.banknifty_details['pe_strike_distance']  , self.banknifty_details['premium_sl_point']  ,
                                                        self.banknifty_details['CE'] , self.banknifty_details['PE'] , self.banknifty_details['timeframe']  , self.banknifty_details['start_method'] ,
                                                        self.banknifty_details['increase_sl_method']  , self.banknifty_details['premium_sl_point']  ,  self.banknifty_details['tp_type'] ,
                                                        self.master_df , self.shoonya , self.fyers , self.angleone , self.get_individual_booked_pl("BANKNIFTY") , self.update_booked_pl , self.load_order , self.logger )

                if not self.banknifty_thread.is_alive():
                    # print(f"thred is not define or not alive " , self.banknifty_thread.is_alive())
                    if not self.banknifty_thread.finished:
                        self.banknifty_thread.tradingmode = self.trading_mode
                        self.banknifty_thread.msg["Entry Type"] = break_market
                        self.banknifty_thread.start()
                        return True, "Order Process Start"
                    else:
                        return  False, "First Save BankNifty Peramter Again Then Process Will be Start After Pressing START Button"
                else:
                    return False, "Order Process Already Running"
                
            else:
                False , f"INDEX {index} not found"
        except Exception as e:
            self.logger.warning(f"Error in {index} run_my_thread")           
            self.logger.warning(e)           


    def increment_sl(self, sl_method , index):
        self.logger.info("nifty SL incrementing")
        try:
            if index == "NIFTY":
                if self.nifty_thread is not None and self.nifty_thread.is_alive():
                    if self.nifty_details['premium_sl_point'] != 0:
                        return False, "First set Zero value at Primum SL"
                    if sl_method in ["50%", "HL"]:
                        if self.nifty_thread.increment_sl(sl_method):
                            self.nifty_details['increase_sl_method'] = sl_method
                            return True, f"{sl_method}  increase successfully"
                        else:
                            return False, "Somthing Wrong in Increment SL"
                    else:
                        return False, "value Error"
                else:
                    return False, "No Running Process Found"
            elif index == "BANKNIFTY":
                if self.banknifty_thread is not None and self.banknifty_thread.is_alive():
                    if self.banknifty_details['premium_sl_point'] != 0:
                        return False, "First set Zero value at Primum SL"
                    if sl_method in ["50%", "HL"]:
                        if self.banknifty_thread.increment_sl(sl_method):
                            self.banknifty_details['increase_sl_method'] = sl_method
                            return True, f"{sl_method}  increase successfully"
                        else:
                            return False, "Somthing Wrong in Increment SL"
                    else:
                        return False, "value Error"
                else:
                    return False, "No Running Process Found"
        except Exception as e:
            self.logger.warning(f"Error in increment_sl {e}")


    def target_qty_update(self , index , value):
        self.logger.info(f"{index} target qty update processing")
        try:
            if index == "NIFTY":
                self.nifty_details['tp_type'] = value
                if self.nifty_thread is not None:
                    self.nifty_thread.msg["TP Type"] = value
                    if self.nifty_thread.msg['TP Type'] == value:
                        return True , "Tranget Method updated in Process"
                    else:
                        return False , "Error in Updating Target Qty"
                return True , "Tranget Method updated in data"

            elif index == "BANKNIFTY":
                self.banknifty_details['tp_type'] = value
                if self.banknifty_thread is not None:
                    self.banknifty_thread.msg["TP Type"] = value
                    if self.banknifty_thread.msg['TP Type'] == value:
                        return True , "Tranget Method updated in Process"
                    else:
                        return False , "Error in Updating Target Qty"
                return True , "Tranget Method updated in data"
            
        except Exception as e:
            self.logger.warning(f"Error in banknifty_target_qty_update: {e}")        
            

    def squreoff(self , index):
        self.logger.info(f"{index} Square off processing")
        try:
            if index == "NIFTY":
                if self.nifty_thread is None:
                    return True, "No Previous Process Found" , self.form_msg
                else:
                    
                    _status , form_msg = self.nifty_thread.squreoff()
                    if _status:
                        self.put_in_async_queue({"display":"table" ,  "msg":self.nifty_thread.msg})
                        self.nifty_thread = None
                        return True, "Process Stop Successfully" ,  form_msg
                    else:
                        return False, "Squreoff not Completed , Please check menually" , form_msg

            elif index == "BANKNIFTY":
                if self.banknifty_thread is None:
                    return True, "No Previous Process Found"
                elif self.banknifty_thread.squreoff():
                    self.put_in_async_queue({"display":"table" ,  "msg": self.banknifty_thread.msg })
                    self.banknifty_thread = None
                    return True, "Process Stop Successfully"
                    # return True, "Process Stop Successfully"
                else:
                    return False, self.banknifty_thread.msg
        except Exception as e:
            self.logger.warning(f"Error in banknifty Squreoff : {e}")


    def check_order_running(self):
        try:
            if ( self.nifty_thread is None or not self.nifty_thread.is_alive() ) and ( self.banknifty_thread is  None or not self.banknifty_thread.is_alive() ):
                return False
            return True    
        except Exception as e:
            self.logger.warning(f"may be any one process running")
            return True

        
