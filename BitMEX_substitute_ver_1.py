import json, schedule, pymysql, os
from bitmex import bitmex
from datetime import datetime, timedelta
from pytz import all_timezones, timezone, utc
from time import monotonic, sleep


import requests, telegram

def Telegram_Alert(msg):

    my_token = '708568812:AAHhjTI3Q1prIpu2wqWMFWCZrNfbOqfhGm0'
    bot = telegram.Bot(token=my_token)
    bot.sendMessage(chat_id='@neuralbc_signalTest', text="[BitMEX Data Manager] %s" % (msg))
    #bot.sendMessage(chat_id=-302610962, text="[BitMEX Data Manager] %s" % (msg))


# def export_to_MongoDB_sub(ToDB):
#     client = MongoClient(host='localhost', port=27017, username="neuralbc-ai", password="neuralbc-ai")
#     DB = client['BITMEX']
#     collection = DB['BTC/USD_5MIN']
#     print("Taken", ToDB)
#     collection.insert_one(ToDB)
#     print("-----------------------------------------------------------------")


def export_to_MySQL(ToDB):
    Query = "INSERT INTO ohlcv_5m(pred_time, price_open, price_high, price_low, price_close, volume) VALUES (STR_TO_DATE(\'%s\',\' %s\'),%.2f, %.2f, %.2f, %.2f, %.2f);" % (ToDB['time_period_end'], "%Y-%m-%d %H:%i:%s", ToDB['price_open'], ToDB['price_high'], ToDB['price_low'], ToDB['price_close'], ToDB['volume'])
    conn = pymysql.connect(host='neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com', user='admin',
                           password='neuralbc', db='bitmex_price', charset='utf8')

    try:
        curs = conn.cursor()
        print(Query)
        curs.execute(Query)
        conn.commit()

    finally:
        conn.close()

def from_UTC_to_SGT(UTC_time):

    Singapore = timezone('Asia/Singapore')
    SingaporeTime = UTC_time.astimezone(Singapore)
    return SingaporeTime.strftime('%Y-%m-%d %H:%M:%S')

def from_SGT_to_UTC(SGTTime):
    Singapore = timezone('Asia/Singapore')
    SGT = Singapore.localize(SGTTime)
    UTC_time = SGT.astimezone(utc)
    return UTC_time



def Conn_REST_BitMEX(StartTime):

    try:
        client = bitmex(test=False, api_key=None, api_secret=None)  ### Risk of 502 Error : Bad Gateway
        StartTime = from_SGT_to_UTC(StartTime)

        print(StartTime)
        trades = \
            client.Trade.Trade_getBucketed(binSize='5m', partial=True, symbol='XBTUSD', count=2,
                                           startTime=StartTime).result()[
                0]
        ## Risk of 503 Error : Service Unavailable
        i = 0
        for element in trades:
            ToDB = {}
            ToDB['time_period_end'] = from_UTC_to_SGT(element['timestamp'])
            ToDB['price_open'] = element['open']
            ToDB['price_high'] = element['high']
            ToDB['price_low'] = element['low']
            ToDB['price_close'] = element['close']
            ToDB['volume'] = element['volume']
            print(i)
            print(ToDB)
            if i == 0:
                try:
                    export_to_MySQL(ToDB)
                except OSError as oe:
                    print(oe)
                    sleep(300)
                    cmd = "python3 ~/BitMEX/BitMEX_substitute_filler.py --year %d --month %d --day %d --hour %d --minute %d" % (
                    element['timestamp'].year, element['timestamp'].month, element['timestamp'].day,
                    element['timestamp'].hour, element['timestamp'].minute)
                    os.system(cmd)
                    break

            i = i + 1
    except Exception as exc:
        print(exc)
        sleep(120)
        Conn_REST_BitMEX(StartTime)








def task_manager():
    sleep(18)

    now = datetime.now()

    #before_now = now - timedelta(seconds=300)
    before_now = now

    if before_now.minute % 5 == 0:
        Minutes = before_now.minute
    else:
        Minutes = before_now.minute - (before_now.minute % 5)

    print(Minutes)


    start_time = datetime(now.year, now.month, now.day, now.hour, Minutes, 0)
    Conn_REST_BitMEX(start_time)
    del now





if __name__=="__main__":



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






