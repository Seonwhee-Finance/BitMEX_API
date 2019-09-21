import json, schedule, os, subprocess
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

def from_UTC_to_SGT(UTC_time):

    Singapore = timezone('Asia/Singapore')
    SingaporeTime = UTC_time.astimezone(Singapore)
    return SingaporeTime.strftime('%Y-%m-%d %H:%M:%S')

def from_SGT_to_UTC(SGTTime):
    Singapore = timezone('Asia/Singapore')
    SGT = Singapore.localize(SGTTime)
    UTC_time = SGT.astimezone(utc)
    return UTC_time

def checkLatestDateTime():
    Query = "SELECT pred_time FROM ohlcv_5m ORDER BY pred_time DESC LIMIT 1;"
    conn = pymysql.connect(host='neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='neuralbc', db='bitmex_price', charset='utf8')

    try:
        curs = conn.cursor()
        curs.execute(Query)
        latest = curs.fetchone()
        conn.commit()

    finally:
        conn.close()

    return latest

def Get_5minute_Term():
    now = datetime.now()
    before_now = now

    if before_now.minute % 5 == 0:
        Minutes = before_now.minute
    else:
        Minutes = before_now.minute - (before_now.minute % 5)
    edited_time = datetime(now.year, now.month, now.day, now.hour, Minutes, 0)

    return edited_time


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

    trades = client.Trade.Trade_getBucketed(binSize='5m', partial=True, symbol='XBTUSD', count=200, startTime=StartTime, endTime=EndTime).result()[0]

    i = 0
    for element in trades:
        ToDB = {}
        ToDB['time_period_end'] = from_UTC_to_SGT(element['timestamp'])
        ToDB['price_open'] = element['open']
        ToDB['price_high'] = element['high']
        ToDB['price_low'] = element['low']
        ToDB['price_close'] = element['close']
        ToDB['volume'] = element['volume']

        export_to_MySQL(ToDB)

        i = i + 1

    #os.system("nohup python3 -u ~/BitMEX/BitMEX_substitute_ver_1.py >> ~/BitMEX/log_5min_candle.txt &")

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


def task_manager():
    ldt = checkLatestDateTime()[0]
    edt = Get_5minute_Term()
    ldt_five = ldt + timedelta(minutes=5)
    if ldt_five < edt:
        Telegram_Alert("DB 검사 결과 가장 최근 데이터가 %s이므로 Insert 작업을 합니다." %(ldt_five.strftime('%Y-%m-%d %H:%M:%S')))
        Conn_REST_BitMEX(ldt_five)
        task_manager()





if __name__=="__main__":

    schedule.every().day.at("00:17").do(task_manager)
    schedule.every().day.at("03:17").do(task_manager)
    schedule.every().day.at("06:17").do(task_manager)

    schedule.every().day.at("18:17").do(task_manager)
    schedule.every().day.at("21:17").do(task_manager)

    while True:
        schedule.run_pending()
        sleep(1)













