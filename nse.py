import requests
import pandas as pd
import gzip
import shutil
from datetime import datetime, timedelta


class load_new_contract:
    def __init__(self):
        self.df = None , None , None
        self.last_update = datetime.now()# - timedelta(hours=9)

    def get_df(self):
        if self.last_update < datetime.now():# + timedelta(hours=8):
            if self.df is (None , None , None):
                self.df = self.get_latest_available_nse_fo_contract()
                self.last_update = datetime.now()
        return self.df

    def get_latest_available_nse_fo_contract(self):
            df = pd.read_csv( "https://public.fyers.in/sym_details/NSE_FO.csv", header=None )
            df.columns = [ "Fytoken", "SymbolDetails", "ExchangeInstrumentType", "MinimumLotSize", "TickSize", "ISIN", "TradingSession", "LastUpdateDate", "ExpiryDate", "SymbolTicker", "Exchange", "Segment", "ScripCode", "UnderlyingSymbol", "UnderlyingScripCode", "StrikePrice", "OptionType", "UnderlyingFyToken", "ReservedColumnString", "ReservedColumnInt", "ReservedColumnFloat" ]
            df = df[ df["UnderlyingSymbol"].isin(["NIFTY", "BANKNIFTY"])]
            df["ExpiryDate"] = pd.to_datetime( df["ExpiryDate"], unit="s", utc=True )
            df["ExpiryDate"] = df["ExpiryDate"].dt.tz_convert( "Asia/Kolkata" )
            df["ExpiryDate"] = df["ExpiryDate"].dt.tz_localize(None)
            df["ExpiryDate"] = df["ExpiryDate"].dt.date
            df = df[["MinimumLotSize" ,"UnderlyingSymbol" ,"OptionType" ,"ExpiryDate" ,"StrikePrice"]]
            df.rename(columns={ "MinimumLotSize": "lot_size", "UnderlyingSymbol": "underlying_symbol", "StrikePrice":"strike" , "OptionType": "option_type", "ExpiryDate": "expiry"}, inplace=True)
            option_df = df[df['option_type'].isin(['CE' , 'PE'])]
            option_df['instrument'] = "OPTIDX"
            option_df['expiry'] = pd.to_datetime(option_df['expiry'])
            
            nifty_df = option_df[ (option_df['underlying_symbol'] == "NIFTY") ]
            nifty_df = nifty_df[ nifty_df['expiry'].isin( nifty_df['expiry'].sort_values().unique()[:3] )]
            
            banknifty_df = option_df[ ( option_df['underlying_symbol'] == "BANKNIFTY") ]
            banknifty_df = banknifty_df[ banknifty_df['expiry'].isin( banknifty_df['expiry'].sort_values().unique()[:2] )]
            
            option_df = pd.concat([nifty_df , banknifty_df])
            future_df = df[df['option_type'] == "XX" ]
            future_df['instrument'] = "FUTIDX"
            future_df['expiry'] = pd.to_datetime(future_df['expiry'])
            future_df = future_df[future_df['expiry'].isin(future_df['expiry'].nsmallest(2))]

            index_df = pd.read_csv( "https://public.fyers.in/sym_details/NSE_CM.csv" , header=None  )
            index_df.columns = [ "Fytoken", "SymbolDetails", "ExchangeInstrumentType", "MinimumLotSize", "TickSize", "ISIN", "TradingSession", "LastUpdateDate", "ExpiryDate", "SymbolTicker", "Exchange", "Segment", "ScripCode", "UnderlyingSymbol", "UnderlyingScripCode", "StrikePrice", "OptionType", "UnderlyingFyToken", "ReservedColumnString", "ReservedColumnInt", "ReservedColumnFloat", ]
            index_df = index_df[  (index_df["ExchangeInstrumentType"] == 10)  &  (index_df["SymbolDetails"].isin(["NIFTY50-INDEX", "NIFTYBANK-INDEX"])) ].reset_index()
            index_df = index_df[[ "MinimumLotSize" , "UnderlyingSymbol"  , "OptionType" , "ExpiryDate" ,"StrikePrice"   ]]
            index_df.rename(columns={ "MinimumLotSize": "lot_size", "UnderlyingSymbol": "underlying_symbol", "StrikePrice":"strike" , "OptionType": "option_type", "ExpiryDate": "expiry"}, inplace=True)
            index_df['instrument'] = "INDEX"
            # df  = pd.concat( [index_df , option_df , future_df ] )
            # df = df.reset_index(drop=True)
            return index_df  , option_df, future_df


# a = load_new_contract()
# z = a.get_latest_available_nse_fo_contract()
# z.expiry.unique()