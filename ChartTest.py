import pymysql
import pandas as pd
import numpy as np
import ccxt, urllib, schedule
import requests

from websocket import create_connection

from pymongo import MongoClient, ASCENDING, DESCENDING
from time import monotonic, sleep
from datetime import datetime, timedelta


def HaHa():
    now = datetime.now()
    date_from = now - timedelta(days=3)
    Signals = []


    Query = "SELECT test_stat_10min_0_1.pred_time, test_stat_10min_0_1.pred FROM rawData_10min INNER JOIN test_stat_10min_0_1 ON test_stat_10min_0_1.pred_time=rawData_10min.pred_time AND (test_stat_10min_0_1.pred=2 OR test_stat_10min_0_1.pred=0) AND (test_stat_10min_0_1.pred_time>='%s');" % (
        date_from)
    conn = pymysql.connect(host='localhost', user='root', password='neuralbc', db='trading', charset='utf8')

    client = MongoClient(host='localhost', port=27017, username="neuralbc-ai", password="neuralbc-ai")
    DB = client['BITMEX']
    collection = DB['BTC/USD_5MIN']
    try:
        curs = conn.cursor()
        curs.execute(Query)
        cols = curs.fetchall()
        for col in cols:
            # print([col[0].strftime('%Y-%m-%d %H:%M:%S'), col[1]])
            for collect in collection.find({'time_period_start': "%s" % (col[0].strftime('%Y-%m-%d %H:%M:%S'))}):
                if col[1] == 2:
                    temp_record = collect['time_period_start']
                elif col[1] == 0:
                    temp_record = collect['time_period_start']
                Signals.append(temp_record)
                del temp_record


    finally:
        conn.close()

    date_from = now - timedelta(days=3)
    date_from_str = date_from.strftime('%Y-%m-%d %H:%M:%S')
    DB_schema = []
    DB_volume = []

    for collect in collection.find({'time_period_start': {"$gte": "%s" % date_from_str}}).sort('time_period_start', ASCENDING):
        temp_record = [collect['time_period_start'], collect['price_low'], collect['price_open'],
                       collect['price_close'], collect['price_high']]

        if collect['time_period_start'] in Signals:
            temp_record_volume = [collect['time_period_start'], collect['volume_traded'], collect['volume_traded']+100000]
        else:
            temp_record_volume = [collect['time_period_start'], collect['volume_traded'], 0]

        # temp_record["Volume"] = collect['volume_traded']
        DB_schema.append(temp_record)
        DB_volume.append(temp_record_volume)
        del temp_record
        del temp_record_volume

    print(Signals)

    for db_vol in DB_volume:
        if db_vol[2] is not 0:
            print(db_vol)





if __name__=="__main__":
    HaHa()