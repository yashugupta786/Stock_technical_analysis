import pandas as pd
import pymongo
from kiteconnect import KiteTicker, KiteConnect
# from support_detection import *
import concurrent.futures
from os import  path
import configparser
from src.Price_action_techniques.support_detection_v1 import *
from src.Price_action_techniques.abc_pattern import *
from src.Price_action_techniques.double_bottom import *
from src.Price_action_techniques.technical_analysis import *
from src.Price_action_techniques.Indicators import *
from src.Price_action_techniques.config import *
from src.Price_action_techniques.fibonacci import *
from src.Price_action_techniques.candles import *

CONFIG_DIR = path.abspath(path.join(__file__, "../../"))
config_file_loc = CONFIG_DIR + "/config/Config.cfg"
config_obj = configparser.ConfigParser()
try:
    config_obj.read(config_file_loc)
    key = str(config_obj.get("Kiteconfig", "kite_api_key"))
    access_token = str(config_obj.get("Kiteconfig", "kite_access_token"))
    mongo_port = (config_obj.get("mongo_details", "MongoPort"))
    weekly_collection = (config_obj.get("mongo_details", "weekly_data_collection"))
    daily_collection = (config_obj.get("mongo_details", "Daily_datastore"))
    nse_dump = str(config_obj.get("mongo_details", "NSE_datadump"))
    mongo_db_connect = str(config_obj.get("mongo_details", "Database_name"))
    Swing_Selection = str(config_obj.get("mongo_details", "Swing_Selection"))

except Exception as e:
    raise Exception("Config file error: " + str(e))

client = pymongo.MongoClient("localhost", 27017)
kite = KiteConnect(api_key=key)
kite.set_access_token(access_token)

# ----DataLoader--------------------------
# -----------
db = client[mongo_db_connect]
nse_coll=db[nse_dump]
weekly_coll=db[weekly_collection]
daily_coll=db[daily_collection]
Swing_Selection_col=db[Swing_Selection]
symbols_list=nse_coll.find({},{"Symbol":1,"Company Name":1,"_id":False})
symbols_list=list(symbols_list)
symbol_list_nse=[i["Symbol"] for i in symbols_list]
company_name=[i["Company Name"] for i in symbols_list]
stock_info=dict(zip(symbol_list_nse,company_name))
# symbol_list_nse=["HDFC","IGL", "ONMOBILE","SANOFI","SUNPHARMA"]
# symbol_list_nse=["NAUKRI"]

class Swing_trade(object):
    def __init__(self):
        self.all_nse=symbol_list_nse
        self.stck_info=stock_info
    def check_volume(self, df):
        df= df.copy()
        df['normalize_vol']= df['volume']/df['volume'].max()
        if df['normalize_vol'].iloc[-1]>0.8:
            return {"Volume": "Very High"}
        elif 0.6 <df['normalize_vol'].iloc[-1] < 0.8:
            return {"Volume": "High"}
        elif df['normalize_vol'].iloc[-1]<0.3:
            return {"Volume": "Low"}
        else:
            return {"Volume": "Avg"}


    def get_daily_data(self,ticker):
        daily_data=list(daily_coll.find({"Name":ticker},{"_id":False}))
        daily_data=pd.DataFrame(daily_data)
        current_day_df=get_Kitedata(ticker,1,"day",kite)
        final_df=pd.concat([daily_data,current_day_df.tail(1)],ignore_index=True)
        return final_df
    def stock_signals(self,ticker):
        try:
            final_result ={}
            stock_daily_df=self.get_daily_data(ticker)
            if len(stock_daily_df)<1:
                return {}
            final_result.update(self.check_volume(stock_daily_df.tail(20)))
            sup_ret=get_support_old(ticker,stock_daily_df)
            final_result.update(sup_ret)
            abc_ret=getabcinfo(ticker,stock_daily_df)
            final_result.update(abc_ret)
            double_bottom_ret=getdoublebottom(ticker,stock_daily_df)
            final_result.update(double_bottom_ret)
            ma44_ret=movingAvg(ticker,stock_daily_df)
            final_result.update(ma44_ret)
            indicator=Trade_indicators()
            RSI=indicator.RSI(stock_daily_df)
            # dict_morning_star=indicator.morning_star(stock_daily_df)
            # final_result.update(dict_morning_star)
            if RSI:
                final_result["RSI"]=RSI
            if not any(v is not None for v in final_result.values()):
                return {}
            lastrec= stock_daily_df.tail(1)[["Name","open","high","low", "close"]].to_dict(orient='records')[0]
            adx=indicator.get_adx(stock_daily_df)
            final_result.update(lastrec)
            final_result['ADX']=adx
            fib_res=get_fibonacci(ticker,stock_daily_df)
            final_result.update(fib_res)
            res_candle=recognize_candlestick(ticker, stock_daily_df.tail(5))
            final_result.update(res_candle)
            final_result.update(supertrend(stock_daily_df))

            return final_result
        except Exception as e:
            print(ticker)
            print(str(e))
            return {}



    def schedule_job(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            results = executor.map(self.stock_signals, self.all_nse)
        return results

    def get_stock_Selection(self):
        selected_stock = pd.DataFrame()
        results = self.schedule_job()
        selected_stock = selected_stock.append(list(results), ignore_index=True)
        selected_stock=selected_stock[selected_stock['Name'].notna()]
        selected_stock["Company name"]=selected_stock["Name"].map(self.stck_info)
        selected_stock=selected_stock[["Company name","Name",'open', 'high', 'low', 'close','Volume',
                                       'On Support', 'On Breakout', 'Current Support', 'Current Resistance',"Support Margin","Resistance Margin", 'LowerBand', '50MA', 'abc',
                                       'Double Bottom', 'IS 44','% MA close to Candle', 'Candle Strength','Candle Type', '44 SMA',
                                       'RSI','ADX',"Trend","candlestick_pattern","candlestick_pattern_trend","candlestick_match_count","fib 0", "fib 24", "fib 38", "fib 50", "fib 62", "fib 79","fib 100"]]

        return selected_stock


obj1= Swing_trade()
selected_stock=obj1.get_stock_Selection()
selected_stock.to_csv(r'Results/selected_swing_stocks.csv',index=False)
selected_stock_data=selected_stock.T.to_dict().values()
Swing_Selection_col.remove()
Swing_Selection_col.insert_many(selected_stock_data)



