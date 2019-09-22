import json, schedule, pymysql, os
from bitmex import bitmex
from datetime import datetime, timedelta
from pytz import all_timezones, timezone, utc
from time import monotonic, sleep


import requests, telegram

def Telegram_Alert(msg):

    my_token = ''
    bot = telegram.Bot(token=my_token)
    bot.sendMessage(chat_id='', text="[New-DB Manager] %s" % (msg))
    




def Comparison_two_prices(TimeStamp, TimeBetween):
    Query1 = "SELECT price_close FROM ohlcv_5m WHERE pred_time=\'%s\';" %(TimeStamp)
    Query2 = "SELECT price_close FROM ohlcv_5m WHERE pred_time=\'%s\';" % (TimeStamp + timedelta(minutes=TimeBetween))
    conn = pymysql.connect(host='', user='',
                           password='', db='', charset='utf8')

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

    conn = pymysql.connect(host='', user='admin',
                           password='', db='', charset='utf8')

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
    try:
        Telegram_Alert(msg_public)
    except telegram.error.TimedOut as te:
        print(te)
    except Exception as ex:
        print(ex)




def Search_Latest_Timepoint(inbetween):

    Query1 = "SELECT pred_time FROM model_validation_%dmin ORDER BY pred_time DESC LIMIT 1;" %(inbetween)
    conn = pymysql.connect(host='', user='',
                           password='', db='bitmex_price', charset='utf8')

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

    #task_manager()



    schedule.every().day.at("00:01").do(task_manager)
    schedule.every().day.at("00:05").do(task_manager)
    schedule.every().day.at("00:10").do(task_manager)
    schedule.every().day.at("00:15").do(task_manager)
    schedule.every().day.at("00:20").do(task_manager)
    schedule.every().day.at("00:25").do(task_manager)
    schedule.every().day.at("00:30").do(task_manager)
    schedule.every().day.at("00:35").do(task_manager)
    schedule.every().day.at("00:40").do(task_manager)
    schedule.every().day.at("00:45").do(task_manager)
    schedule.every().day.at("00:50").do(task_manager)
    schedule.every().day.at("00:55").do(task_manager)

    schedule.every().day.at("01:00").do(task_manager)
    schedule.every().day.at("01:05").do(task_manager)
    schedule.every().day.at("01:10").do(task_manager)
    schedule.every().day.at("01:15").do(task_manager)
    schedule.every().day.at("01:20").do(task_manager)
    schedule.every().day.at("01:25").do(task_manager)
    schedule.every().day.at("01:30").do(task_manager)
    schedule.every().day.at("01:35").do(task_manager)
    schedule.every().day.at("01:40").do(task_manager)
    schedule.every().day.at("01:45").do(task_manager)
    schedule.every().day.at("01:50").do(task_manager)
    schedule.every().day.at("01:55").do(task_manager)

    schedule.every().day.at("02:00").do(task_manager)
    schedule.every().day.at("02:05").do(task_manager)
    schedule.every().day.at("02:10").do(task_manager)
    schedule.every().day.at("02:15").do(task_manager)
    schedule.every().day.at("02:20").do(task_manager)
    schedule.every().day.at("02:25").do(task_manager)
    schedule.every().day.at("02:30").do(task_manager)
    schedule.every().day.at("02:35").do(task_manager)
    schedule.every().day.at("02:40").do(task_manager)
    schedule.every().day.at("02:45").do(task_manager)
    schedule.every().day.at("02:50").do(task_manager)
    schedule.every().day.at("02:55").do(task_manager)

    schedule.every().day.at("03:00").do(task_manager)
    schedule.every().day.at("03:05").do(task_manager)
    schedule.every().day.at("03:10").do(task_manager)
    schedule.every().day.at("03:15").do(task_manager)
    schedule.every().day.at("03:20").do(task_manager)
    schedule.every().day.at("03:25").do(task_manager)
    schedule.every().day.at("03:30").do(task_manager)
    schedule.every().day.at("03:35").do(task_manager)
    schedule.every().day.at("03:40").do(task_manager)
    schedule.every().day.at("03:45").do(task_manager)
    schedule.every().day.at("03:50").do(task_manager)
    schedule.every().day.at("03:55").do(task_manager)

    schedule.every().day.at("04:00").do(task_manager)
    schedule.every().day.at("04:05").do(task_manager)
    schedule.every().day.at("04:10").do(task_manager)
    schedule.every().day.at("04:15").do(task_manager)
    schedule.every().day.at("04:20").do(task_manager)
    schedule.every().day.at("04:25").do(task_manager)
    schedule.every().day.at("04:30").do(task_manager)
    schedule.every().day.at("04:35").do(task_manager)
    schedule.every().day.at("04:40").do(task_manager)
    schedule.every().day.at("04:45").do(task_manager)
    schedule.every().day.at("04:50").do(task_manager)
    schedule.every().day.at("04:55").do(task_manager)

    schedule.every().day.at("05:00").do(task_manager)
    schedule.every().day.at("05:05").do(task_manager)
    schedule.every().day.at("05:10").do(task_manager)
    schedule.every().day.at("05:15").do(task_manager)
    schedule.every().day.at("05:20").do(task_manager)
    schedule.every().day.at("05:25").do(task_manager)
    schedule.every().day.at("05:30").do(task_manager)
    schedule.every().day.at("05:35").do(task_manager)
    schedule.every().day.at("05:40").do(task_manager)
    schedule.every().day.at("05:45").do(task_manager)
    schedule.every().day.at("05:50").do(task_manager)
    schedule.every().day.at("05:55").do(task_manager)

    schedule.every().day.at("06:00").do(task_manager)
    schedule.every().day.at("06:05").do(task_manager)
    schedule.every().day.at("06:10").do(task_manager)
    schedule.every().day.at("06:15").do(task_manager)
    schedule.every().day.at("06:20").do(task_manager)
    schedule.every().day.at("06:25").do(task_manager)
    schedule.every().day.at("06:30").do(task_manager)
    schedule.every().day.at("06:35").do(task_manager)
    schedule.every().day.at("06:40").do(task_manager)
    schedule.every().day.at("06:45").do(task_manager)
    schedule.every().day.at("06:50").do(task_manager)
    schedule.every().day.at("06:55").do(task_manager)

    schedule.every().day.at("07:00").do(task_manager)
    schedule.every().day.at("07:05").do(task_manager)
    schedule.every().day.at("07:10").do(task_manager)
    schedule.every().day.at("07:15").do(task_manager)
    schedule.every().day.at("07:20").do(task_manager)
    schedule.every().day.at("07:25").do(task_manager)
    schedule.every().day.at("07:30").do(task_manager)
    schedule.every().day.at("07:35").do(task_manager)
    schedule.every().day.at("07:40").do(task_manager)
    schedule.every().day.at("07:45").do(task_manager)
    schedule.every().day.at("07:50").do(task_manager)
    schedule.every().day.at("07:55").do(task_manager)

    schedule.every().day.at("08:01").do(task_manager)
    schedule.every().day.at("08:05").do(task_manager)
    schedule.every().day.at("08:10").do(task_manager)
    schedule.every().day.at("08:15").do(task_manager)
    schedule.every().day.at("08:20").do(task_manager)
    schedule.every().day.at("08:25").do(task_manager)
    schedule.every().day.at("08:30").do(task_manager)
    schedule.every().day.at("08:35").do(task_manager)
    schedule.every().day.at("08:40").do(task_manager)
    schedule.every().day.at("08:45").do(task_manager)
    schedule.every().day.at("08:50").do(task_manager)
    schedule.every().day.at("08:55").do(task_manager)

    schedule.every().day.at("09:00").do(task_manager)
    schedule.every().day.at("09:05").do(task_manager)
    schedule.every().day.at("09:10").do(task_manager)
    schedule.every().day.at("09:15").do(task_manager)
    schedule.every().day.at("09:20").do(task_manager)
    schedule.every().day.at("09:25").do(task_manager)
    schedule.every().day.at("09:30").do(task_manager)
    schedule.every().day.at("09:35").do(task_manager)
    schedule.every().day.at("09:40").do(task_manager)
    schedule.every().day.at("09:45").do(task_manager)
    schedule.every().day.at("09:50").do(task_manager)
    schedule.every().day.at("09:55").do(task_manager)

    schedule.every().day.at("10:00").do(task_manager)
    schedule.every().day.at("10:05").do(task_manager)
    schedule.every().day.at("10:10").do(task_manager)
    schedule.every().day.at("10:15").do(task_manager)
    schedule.every().day.at("10:20").do(task_manager)
    schedule.every().day.at("10:25").do(task_manager)
    schedule.every().day.at("10:30").do(task_manager)
    schedule.every().day.at("10:35").do(task_manager)
    schedule.every().day.at("10:40").do(task_manager)
    schedule.every().day.at("10:45").do(task_manager)
    schedule.every().day.at("10:50").do(task_manager)
    schedule.every().day.at("10:55").do(task_manager)

    schedule.every().day.at("11:00").do(task_manager)
    schedule.every().day.at("11:05").do(task_manager)
    schedule.every().day.at("11:10").do(task_manager)
    schedule.every().day.at("11:15").do(task_manager)
    schedule.every().day.at("11:20").do(task_manager)
    schedule.every().day.at("11:25").do(task_manager)
    schedule.every().day.at("11:30").do(task_manager)
    schedule.every().day.at("11:35").do(task_manager)
    schedule.every().day.at("11:40").do(task_manager)
    schedule.every().day.at("11:45").do(task_manager)
    schedule.every().day.at("11:50").do(task_manager)
    schedule.every().day.at("11:55").do(task_manager)

    schedule.every().day.at("12:00").do(task_manager)
    schedule.every().day.at("12:05").do(task_manager)
    schedule.every().day.at("12:10").do(task_manager)
    schedule.every().day.at("12:15").do(task_manager)
    schedule.every().day.at("12:20").do(task_manager)
    schedule.every().day.at("12:25").do(task_manager)
    schedule.every().day.at("12:30").do(task_manager)
    schedule.every().day.at("12:35").do(task_manager)
    schedule.every().day.at("12:40").do(task_manager)
    schedule.every().day.at("12:45").do(task_manager)
    schedule.every().day.at("12:50").do(task_manager)
    schedule.every().day.at("12:55").do(task_manager)

    schedule.every().day.at("13:00").do(task_manager)
    schedule.every().day.at("13:05").do(task_manager)
    schedule.every().day.at("13:10").do(task_manager)
    schedule.every().day.at("13:15").do(task_manager)
    schedule.every().day.at("13:20").do(task_manager)
    schedule.every().day.at("13:25").do(task_manager)
    schedule.every().day.at("13:30").do(task_manager)
    schedule.every().day.at("13:35").do(task_manager)
    schedule.every().day.at("13:40").do(task_manager)
    schedule.every().day.at("13:45").do(task_manager)
    schedule.every().day.at("13:50").do(task_manager)
    schedule.every().day.at("13:55").do(task_manager)

    schedule.every().day.at("14:00").do(task_manager)
    schedule.every().day.at("14:05").do(task_manager)
    schedule.every().day.at("14:10").do(task_manager)
    schedule.every().day.at("14:15").do(task_manager)
    schedule.every().day.at("14:20").do(task_manager)
    schedule.every().day.at("14:25").do(task_manager)
    schedule.every().day.at("14:30").do(task_manager)
    schedule.every().day.at("14:35").do(task_manager)
    schedule.every().day.at("14:40").do(task_manager)
    schedule.every().day.at("14:45").do(task_manager)
    schedule.every().day.at("14:50").do(task_manager)
    schedule.every().day.at("14:55").do(task_manager)

    schedule.every().day.at("15:00").do(task_manager)
    schedule.every().day.at("15:05").do(task_manager)
    schedule.every().day.at("15:10").do(task_manager)
    schedule.every().day.at("15:15").do(task_manager)
    schedule.every().day.at("15:20").do(task_manager)
    schedule.every().day.at("15:25").do(task_manager)
    schedule.every().day.at("15:30").do(task_manager)
    schedule.every().day.at("15:35").do(task_manager)
    schedule.every().day.at("15:40").do(task_manager)
    schedule.every().day.at("15:45").do(task_manager)
    schedule.every().day.at("15:50").do(task_manager)
    schedule.every().day.at("15:55").do(task_manager)

    schedule.every().day.at("16:00").do(task_manager)
    schedule.every().day.at("16:05").do(task_manager)
    schedule.every().day.at("16:10").do(task_manager)
    schedule.every().day.at("16:15").do(task_manager)
    schedule.every().day.at("16:20").do(task_manager)
    schedule.every().day.at("16:25").do(task_manager)
    schedule.every().day.at("16:30").do(task_manager)
    schedule.every().day.at("16:35").do(task_manager)
    schedule.every().day.at("16:40").do(task_manager)
    schedule.every().day.at("16:45").do(task_manager)
    schedule.every().day.at("16:50").do(task_manager)
    schedule.every().day.at("16:55").do(task_manager)

    schedule.every().day.at("17:00").do(task_manager)
    schedule.every().day.at("17:05").do(task_manager)
    schedule.every().day.at("17:10").do(task_manager)
    schedule.every().day.at("17:15").do(task_manager)
    schedule.every().day.at("17:20").do(task_manager)
    schedule.every().day.at("17:25").do(task_manager)
    schedule.every().day.at("17:30").do(task_manager)
    schedule.every().day.at("17:35").do(task_manager)
    schedule.every().day.at("17:40").do(task_manager)
    schedule.every().day.at("17:45").do(task_manager)
    schedule.every().day.at("17:50").do(task_manager)
    schedule.every().day.at("17:55").do(task_manager)

    schedule.every().day.at("18:01").do(task_manager)
    schedule.every().day.at("18:05").do(task_manager)
    schedule.every().day.at("18:10").do(task_manager)
    schedule.every().day.at("18:15").do(task_manager)
    schedule.every().day.at("18:20").do(task_manager)
    schedule.every().day.at("18:25").do(task_manager)
    schedule.every().day.at("18:30").do(task_manager)
    schedule.every().day.at("18:35").do(task_manager)
    schedule.every().day.at("18:40").do(task_manager)
    schedule.every().day.at("18:45").do(task_manager)
    schedule.every().day.at("18:50").do(task_manager)
    schedule.every().day.at("18:55").do(task_manager)

    schedule.every().day.at("19:00").do(task_manager)
    schedule.every().day.at("19:05").do(task_manager)
    schedule.every().day.at("19:10").do(task_manager)
    schedule.every().day.at("19:15").do(task_manager)
    schedule.every().day.at("19:20").do(task_manager)
    schedule.every().day.at("19:25").do(task_manager)
    schedule.every().day.at("19:30").do(task_manager)
    schedule.every().day.at("19:35").do(task_manager)
    schedule.every().day.at("19:40").do(task_manager)
    schedule.every().day.at("19:45").do(task_manager)
    schedule.every().day.at("19:50").do(task_manager)
    schedule.every().day.at("19:55").do(task_manager)

    schedule.every().day.at("20:00").do(task_manager)
    schedule.every().day.at("20:05").do(task_manager)
    schedule.every().day.at("20:10").do(task_manager)
    schedule.every().day.at("20:15").do(task_manager)
    schedule.every().day.at("20:20").do(task_manager)
    schedule.every().day.at("20:25").do(task_manager)
    schedule.every().day.at("20:30").do(task_manager)
    schedule.every().day.at("20:35").do(task_manager)
    schedule.every().day.at("20:40").do(task_manager)
    schedule.every().day.at("20:45").do(task_manager)
    schedule.every().day.at("20:50").do(task_manager)
    schedule.every().day.at("20:55").do(task_manager)

    schedule.every().day.at("21:00").do(task_manager)
    schedule.every().day.at("21:05").do(task_manager)
    schedule.every().day.at("21:10").do(task_manager)
    schedule.every().day.at("21:15").do(task_manager)
    schedule.every().day.at("21:20").do(task_manager)
    schedule.every().day.at("21:25").do(task_manager)
    schedule.every().day.at("21:30").do(task_manager)
    schedule.every().day.at("21:35").do(task_manager)
    schedule.every().day.at("21:40").do(task_manager)
    schedule.every().day.at("21:45").do(task_manager)
    schedule.every().day.at("21:50").do(task_manager)
    schedule.every().day.at("21:55").do(task_manager)

    schedule.every().day.at("22:00").do(task_manager)
    schedule.every().day.at("22:05").do(task_manager)
    schedule.every().day.at("22:10").do(task_manager)
    schedule.every().day.at("22:15").do(task_manager)
    schedule.every().day.at("22:20").do(task_manager)
    schedule.every().day.at("22:25").do(task_manager)
    schedule.every().day.at("22:30").do(task_manager)
    schedule.every().day.at("22:35").do(task_manager)
    schedule.every().day.at("22:40").do(task_manager)
    schedule.every().day.at("22:45").do(task_manager)
    schedule.every().day.at("22:50").do(task_manager)
    schedule.every().day.at("22:55").do(task_manager)

    schedule.every().day.at("23:00").do(task_manager)
    schedule.every().day.at("23:05").do(task_manager)
    schedule.every().day.at("23:10").do(task_manager)
    schedule.every().day.at("23:15").do(task_manager)
    schedule.every().day.at("23:20").do(task_manager)
    schedule.every().day.at("23:25").do(task_manager)
    schedule.every().day.at("23:30").do(task_manager)
    schedule.every().day.at("23:35").do(task_manager)
    schedule.every().day.at("23:40").do(task_manager)
    schedule.every().day.at("23:45").do(task_manager)
    schedule.every().day.at("23:50").do(task_manager)
    schedule.every().day.at("23:55").do(task_manager)




    while True:
        schedule.run_pending()
        sleep(1)






