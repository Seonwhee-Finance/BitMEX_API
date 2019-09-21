import json, schedule, pymysql, os
from bitmex import bitmex
from datetime import datetime, timedelta
from pytz import all_timezones, timezone, utc
from time import monotonic, sleep


import requests, telegram

def Telegram_Alert(msg):

    my_token = '708568812:AAHhjTI3Q1prIpu2wqWMFWCZrNfbOqfhGm0'
    bot = telegram.Bot(token=my_token)
    bot.sendMessage(chat_id='@neuralbc_signalTest', text="[New-DB Manager] %s" % (msg))
    bot.sendMessage(chat_id=-325656420, text="[BitMEX Bot] %s" % (msg))




def Comparison_two_prices(TimeStamp, TimeBetween):
    Query1 = "SELECT price_close FROM ohlcv_5m WHERE pred_time=\'%s\';" %(TimeStamp)
    Query2 = "SELECT price_close FROM ohlcv_5m WHERE pred_time=\'%s\';" % (TimeStamp + timedelta(minutes=TimeBetween))
    conn = pymysql.connect(host='neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='neuralbc', db='bitmex_price', charset='utf8')

    try:
        curs = conn.cursor()
        curs.execute(Query1)
        initialPrice = curs.fetchone()[0]
        conn.commit()
        curs.execute(Query2)
        finalPrice = curs.fetchone()[0]
        conn.commit()

    finally:
        conn.close()

    return initialPrice, finalPrice

def Get_basic_information(inbetween, timeStamp):

    init, fin = Comparison_two_prices(timeStamp, inbetween)

    conn = pymysql.connect(host='neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='neuralbc', db='bitmex_price', charset='utf8')

    Query1 = "SELECT model_name, pred, steady_percent FROM model_output_%dmin WHERE pred_time=\'%s\';" %(inbetween, timeStamp)

    try:
        curs = conn.cursor()
        curs.execute(Query1)
        row = curs.fetchone()
        conn.commit()
        modelName = row[0]
        modelPred = row[1]
        steadyPercent = row[2]

        if modelPred == 0:
            msg0 = "상승"
        elif modelPred == 1:
            msg0 = "보합"
        elif modelPred == 2:
            msg0 = "하락"

        KST = timeStamp + timedelta(hours=1)

        #msg0 = "%d분 전[%s, %.2f] 모델 %s의 예측결과는 %s이었고, " %(inbetween, KST.strftime('%Y-%m-%d %H:%M:%S'), init,  modelName, msg0)
        msg_public = "%d분 전[%s, %.2f] 예측결과는 %s이었고, " %(inbetween, KST.strftime('%Y-%m-%d %H:%M:%S'), init, msg0)

        #if fin > init * (1 + (steadyPercent / 200)):
        if fin > init:
            actual = 0
            msg1 = "상승[%.2f]하였습니다." %(fin)
        #elif fin < init * (1 - (steadyPercent / 200)):
        elif fin < init:
            actual = 2
            msg1 = "하락[%.2f]하였습니다" %(fin)
        else:
            actual = 1
            msg1 = "보합[%.2f]입니다" %(fin)

        if modelPred == actual:
            correctness = "TRUE"
            msg = "%s \n실제로 %s \n적중하였습니다." %(msg0, msg1)
            msg_public = "%s \n실제로 %s \n적중하였습니다." %(msg_public, msg1)
        else:
            correctness = "FALSE"
            msg = "%s \n그러나 실제로는 %s \n적중 실패하였습니다." %(msg0, msg1)
            msg_public = "%s \n그러나 실제로는 %s \n적중 실패하였습니다." % (msg_public, msg1)

        Query2 = "INSERT INTO model_validation_%dmin(pred_time, model_name, pred, init_price, fin_price, actual, correct) VALUES (\'%s\', \'%s\', %d, %.2f, %.2f, %d, \'%s\')" % (
        inbetween, timeStamp, modelName, modelPred, init, fin, actual, correctness)

        curs.execute(Query2)
        conn.commit()

    finally:
        conn.close()

    #Telegram_Alert(msg)
    #Telegram_Alert(msg_public)




def Search_Latest_Timepoint(inbetween):

    Query1 = "SELECT pred_time FROM model_validation_%dmin ORDER BY pred_time DESC LIMIT 1;" %(inbetween)
    conn = pymysql.connect(host='neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='neuralbc', db='bitmex_price', charset='utf8')

    try:
        curs = conn.cursor()
        curs.execute(Query1)
        Latest_Time = curs.fetchone()[0]
        print(Latest_Time)
        conn.commit()
        Query2 = "SELECT pred_time FROM model_output_%dmin WHERE pred_time>\'%s\' ORDER BY pred_time LIMIT 1;" % (
            inbetween, Latest_Time)
        curs.execute(Query2)
        New_Time = curs.fetchone()[0]
        print(New_Time)
        conn.commit()


    finally:
        conn.close()

    try:
        Get_basic_information(inbetween, New_Time)
    except TypeError as te:
        print("The time has not been come!")
    ###Get_basic_information(inbetween, datetime(2019, 7, 22, 16, 15, 0))





def task_manager():
    try:
        Search_Latest_Timepoint(30)
    except TypeError as te:
        print("This is the latest prediction!!")

    try:
        Search_Latest_Timepoint(20)
    except TypeError as te:
        print("This is the latest prediction!!")

    try:
        Search_Latest_Timepoint(10)
    except TypeError as te:
        print("This is the latest prediction!!")


if __name__=="__main__":

    while True:
        task_manager()
        sleep(1)






