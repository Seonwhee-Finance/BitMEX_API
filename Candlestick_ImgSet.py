import pandas as pd
import boto3
import pymysql
import matplotlib
matplotlib.use('Agg')
import rds_config
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import time, tzinfo, timedelta, datetime
from math import pi
from matplotlib.finance import candlestick2_ohlc, volume_overlay

from time import monotonic, sleep


rds_host  = ""
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

try:
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    sys.exit()
# finally:
#     conn.close()

# The indicators are added to the dataframe itself through the functions below.
def ema(df, n):
    price = df['close']
    price = price.fillna(method='ffill')
    EMA = pd.Series(price.ewm(span=n, min_periods=n - 1).mean(), name='EMA_' + str(n))
    df = df.join(EMA)
    return df


def bollinger(df, n):
    price = df['close']
    price = price.fillna(method='ffill')
    numsd = 2
    """ returns average, upper band, and lower band"""
    df['bbupper_' + str(n)] = price.ewm(span=n, min_periods=n - 1).mean() + 2 * price.rolling(min_periods=n, window=n,
                                                                                              center=False).std()
    df['bblower_' + str(n)] = price.ewm(span=n, min_periods=n - 1).mean() - 2 * price.rolling(min_periods=n, window=n,
                                                                                              center=False).std()

    return df


# Interpret the indicator parameters and add the indicators to the chart:
def add_indicators(indicators_params_list, df, chart):
    for indicator_params in indicators_params_list:
        if indicator_params['name'] == 'ema':
            period = indicator_params['period']
            df = ema(df, period)
            chart.line(df.Date, df['EMA_' + str(period)], line_dash=(4, 4), color='black', alpha=0.7,
                       legend='EMA ' + str(period))
        elif indicator_params['name'] == 'bollinger':
            period = indicator_params['period']
            df = bollinger(df, period)
            chart.line(df.Date, df['bbupper_' + str(period)], color='red', alpha=0.7, legend='bbupper ' + str(period))
            chart.line(df.Date, df['bblower_' + str(period)], color='black', alpha=0.7, legend='blower ' + str(period))

    return chart


def DF_from_DB(presentTime, hourAfter):

    DB_schema = []
    Steady = 0.01



    with conn.cursor() as cur:
        query = "SELECT * FROM ohlcv_5m WHERE pred_time >=\'%s\' AND pred_time <=\'%s\';" %(presentTime, presentTime+timedelta(hours=hourAfter))
        cur.execute(query)

        i = 0

        for row in cur:
            #item_count += 1
            #logger.info(row)
            temp_record = {}
            temp_record['time'] = row[1]
            temp_record['open'] = row[2]
            temp_record['high'] = row[3]
            temp_record['low'] = row[4]
            temp_record['close'] = row[5]
            temp_record['volume'] = row[6]
            # if i == len(cur):
            #     CurrentPrice = row[5]
            #     print(CurrentPrice)



            #originalTable[row[1].strftime("%Y-%m-%d %H:%M:%S")] = row[2:]

            DB_schema.append(temp_record)
            del temp_record
            i = i + 1
        CurrentPrice = DB_schema[-1]['close']

        query2 = "SELECT * FROM ohlcv_5m WHERE pred_time =\'%s\';" %(presentTime+timedelta(hours=hourAfter*2))
        cur.execute(query2)
        for k in cur.fetchall():
            EndPrice = k[5]


        UpperLimit = CurrentPrice + CurrentPrice*Steady
        LowerLimit = CurrentPrice - CurrentPrice*Steady

        if EndPrice > UpperLimit:
            Label = "rise"
        elif EndPrice < LowerLimit:
            Label = "fall"
        elif EndPrice <= UpperLimit and EndPrice >= LowerLimit:
            Label = "steady"

    DF = pd.DataFrame(DB_schema)

    return DF, Label


def Making_OHLCV_Plot(dateTimeStamp):
    

    DF, Label = DF_from_DB(dateTimeStamp, 2)
    DF['mdate'] = [mdates.date2num(d) for d in DF['time']]


    fig = plt.figure(figsize = (6.4,6.4))

    # axis 1
    ax1 = fig.add_subplot(1, 1, 1)

    # plot the candlesticks

    candlestick2_ohlc(ax1, DF['open'], DF['high'], DF['low'], DF['close'], width=.6, colorup='green', colordown='red')

    ax2 = ax1.twinx()
    # set the position of ax2 so that it is short (y2=0.32) but otherwise the same size as ax1
    ax2.set_position(matplotlib.transforms.Bbox([[0.125, 0.1], [0.9, 0.32]]))
    ax2.yaxis.set_label_position("right")
    vc = volume_overlay(ax2, DF['open'], DF['close'], DF['volume'], colorup='green', colordown='red', width=.6)
    ax2.add_collection(vc)

    #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

    ax1.axis('off')
    ax2.axis('off')

    # Saving image

    initStr = dateTimeStamp.strftime('%Y-%m-%d %H:%M:%S')
    initStr = initStr.replace(':', '_')
    initStr = initStr.replace(' ', '_')
    PNG_file = '%s_OHLC.png' %(initStr)
    PNG_file = '/home/ubuntu/TRAIN_DIR/%s/%s' %(Label, PNG_file)
    del initStr








    plt.savefig(PNG_file)
    del PNG_file
    #To_S3_storage('./OHLC.png')






def To_S3_storage(fileToUpload):
    now = datetime.now()
    savetime = now.strftime("%Y-%m-%d_%H_%M_%S")
    s3 = boto3.client('s3')

    s3.upload_file(fileToUpload, '', '%s_OHLC.png'%savetime)


def main():
    initTime = datetime(2018, 1, 17, 15, 0, 0)
    Making_OHLCV_Plot(initTime)

    ### 초기 기준 시간에서 10분씩 이동하면서 이미지를 만든다
    for i in range(30000):
        initTime = initTime + timedelta(minutes=10)
        Making_OHLCV_Plot(initTime)







if __name__=="__main__":
    main()