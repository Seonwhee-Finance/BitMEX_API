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
from matplotlib.finance import candlestick2_ohlc

from time import monotonic, sleep


rds_host  = "neuralbc-ai-db-bitmex.cwgoprvlrqva.ap-northeast-2.rds.amazonaws.com"
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



    with conn.cursor() as cur:
        query = "SELECT * FROM ohlcv_5m WHERE pred_time >=\'%s\' AND pred_time <=\'%s\';" %(presentTime, presentTime+timedelta(hours=hourAfter))
        cur.execute(query)

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
            #print(row[1:])
            #originalTable[row[1].strftime("%Y-%m-%d %H:%M:%S")] = row[2:]

            DB_schema.append(temp_record)
            del temp_record





    DF = pd.DataFrame(DB_schema)

    return DF


def Making_OHLCV_Plot():
    

    DF = DF_from_DB(datetime(2019, 6, 17, 17, 0, 0), 2)
    DF['mdate'] = [mdates.date2num(d) for d in DF['time']]

    print(DF)



    #f1, ax = plt.subplots(figsize = (200,50))
    f1, ax = plt.subplots()
    # plot the candlesticks

    candlestick2_ohlc(ax, DF['open'], DF['high'], DF['low'], DF['close'], width=.6, colorup='green', colordown='red')

    #ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

    # Saving image

    plt.savefig('OHLC.png')
    To_S3_storage('./OHLC.png')






def To_S3_storage(fileToUpload):
    now = datetime.now()
    savetime = now.strftime("%Y-%m-%d_%H_%M_%S")
    s3 = boto3.client('s3')

    s3.upload_file(fileToUpload, 'team-ai-lambda', '%s_OHLC.png'%savetime)


def main():
    Making_OHLCV_Plot()

if __name__=="__main__":
    main()