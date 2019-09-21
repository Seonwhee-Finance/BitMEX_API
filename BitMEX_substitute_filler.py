import json, schedule, argparse, os
import pymysql, telegram
from bitmex_websocket import BitMEXWebsocket
from bitmex import bitmex
from datetime import datetime, timedelta
from pytz import all_timezones, timezone, utc
#from pymongo import MongoClient
from time import monotonic, sleep

import requests

def Telegram_Alert(msg):

    my_token = '708568812:AAHhjTI3Q1prIpu2wqWMFWCZrNfbOqfhGm0'
    bot = telegram.Bot(token=my_token)
    bot.sendMessage(chat_id='@neuralbc_signalTest', text="[BitMEX Data Manager] %s" % (msg))
    #bot.sendMessage(chat_id=-302610962, text="[BitMEX Data Manager] %s" % (msg))

def export_to_MongoDB_sub(ToDB):
    client = MongoClient(host='localhost', port=27017, username="neuralbc-ai", password="neuralbc-ai")
    DB = client['BITMEX']
    collection = DB['BTC/USD_5MIN']
    print("Taken", ToDB)
    collection.insert_one(ToDB)
    print("-----------------------------------------------------------------")

def from_UTC_to_SGT(UTC_time):

    Singapore = timezone('Asia/Singapore')
    SingaporeTime = UTC_time.astimezone(Singapore)
    return SingaporeTime.strftime('%Y-%m-%d %H:%M:%S')

def from_SGT_to_UTC(SGTTime):
    Singapore = timezone('Asia/Singapore')
    SGT = Singapore.localize(SGTTime)
    UTC_time = SGT.astimezone(utc)
    return UTC_time

def export_to_MySQL(ToDB):
    Query = "INSERT INTO ohlcv_5m(pred_time, price_open, price_high, price_low, price_close, volume) VALUES (STR_TO_DATE(\'%s\',\' %s\'),%.2f, %.2f, %.2f, %.2f, %.2f);" % (
    ToDB['time_period_end'], "%Y-%m-%d %H:%i:%s", ToDB['price_open'], ToDB['price_high'], ToDB['price_low'],
    ToDB['price_close'], ToDB['volume'])
    conn = pymysql.connect(host='neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='neuralbc', db='bitmex_price', charset='utf8')

    try:
        curs = conn.cursor()
        print(Query)
        curs.execute(Query)
        conn.commit()

    finally:
        conn.close()

def Conn_REST_BitMEX(StartTime):
    now = datetime.now()
    before_now = now

    if before_now.minute % 5 == 0:
        Minutes = before_now.minute
    else:
        Minutes = before_now.minute - (before_now.minute % 5)

    client = bitmex(test=False, api_key= None, api_secret = None)
    StartTime = from_SGT_to_UTC(StartTime)
    EndTime = from_SGT_to_UTC(datetime(before_now.year, before_now.month, before_now.day, before_now.hour, Minutes, 0))


    #print(StartTime)

    trades = client.Trade.Trade_getBucketed(binSize='5m', partial=True, symbol='XBTUSD', count=1, startTime=StartTime, endTime=EndTime).result()[0]

    i = 0
    for element in trades:
        ToDB = {}
        ToDB['time_period_end'] = from_UTC_to_SGT(element['timestamp'])
        ToDB['price_open'] = element['open']
        ToDB['price_high'] = element['high']
        ToDB['price_low'] = element['low']
        ToDB['price_close'] = element['close']
        ToDB['volume'] = element['volume']
        #print(i)
        #print(ToDB)
        #export_to_MongoDB_sub(ToDB)
        # try:
        #     Telegram_Alert("Missing Data %s (SGT) Filled" %(ToDB['time_period_start']))
        # except Exception as e:
        #     print(e)
        #     sleep(30)
        #     continue
        export_to_MySQL(ToDB)
        #if i == 0:
            #print("Taken")
            #print(ToDB)
            #export_to_MongoDB_sub(ToDB)
        #else:
            #export_to_MongoDB_sub(ToDB)
        i = i + 1

    #os.system("nohup python3 -u ~/BitMEX/BitMEX_substitute_ver_1.py >> ~/BitMEX/log_5min_candle.txt &")



if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int,
                        help="Year")
    parser.add_argument('--month', type=int,
                        help="Month")
    parser.add_argument('--day', type=int,
                        help="Day")
    parser.add_argument('--hour', type=int,
                        help="Hour")
    parser.add_argument('--minute', type=int,
                        help="Minute")

    args = parser.parse_args()

    start_time = datetime(args.year, args.month, args.day, args.hour, args.minute, 0)
    Conn_REST_BitMEX(start_time)












