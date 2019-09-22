import pymysql, telegram
from time import sleep
from datetime import datetime, timedelta
from pytz import all_timezones, timezone, utc
from bitmex_websocket import BitMEXWebsocket

# Basic use of websocket.
def show_as_Seoul_time(SGT):
    Seoul = timezone('Asia/Seoul')
    Singapore = timezone('Asia/Singapore')
    SingaporeTime = Singapore.localize(SGT)
    SeoulTime = SingaporeTime.astimezone(Seoul)
    return SeoulTime.strftime("%Y-%m-%d %H:%M:%S")

def Telegram_Alert(msg):

    my_token = ''
    bot = telegram.Bot(token=my_token)

    bot.sendMessage(chat_id='', text="[BitMEX WebSocket] %s" % (msg) )


def export_to_MySQL(TradeList):
    Singapore = timezone('Asia/Singapore')
    for individual_rec in TradeList:
        print(individual_rec)
        strTotz = datetime.strptime(individual_rec['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
        utcTime = utc.localize(strTotz)
        SGT = utcTime.astimezone(Singapore)
        SGT = SGT.strftime('%Y-%m-%d %H:%M:%S.%f')

        check_Query = "SELECT EXISTS(SELECT * FROM realtime_tick WHERE trd_match_id=\'%s\')AS success;" % (individual_rec['trdMatchID'])

        Query = "INSERT INTO realtime_tick(trd_timestamp, price, side, trd_size, tick_direction, gross_value, foreign_national, home_national, trd_match_id) VALUES (\'%s\',%.2f, \'%s\', %.2f, \'%s\', %.2f, %d, %.7f, \'%s\');" % (
        SGT, individual_rec['price'], individual_rec['side'], individual_rec['size'], individual_rec['tickDirection'], individual_rec['grossValue'],
        individual_rec['foreignNotional'], individual_rec['homeNotional'], individual_rec['trdMatchID'])


        conn = pymysql.connect(host='', user='',
                               password='', db='bitmex_price', charset='utf8')

        try:
            curs = conn.cursor()

            curs.execute(check_Query)
            returnValue = curs.fetchall()[0]
            print(returnValue[0])
            if returnValue[0] == 0:
                curs.execute(Query)
                print(Query)
                #Telegram_Alert("%s Price %.2f, Side %s, Size %.2f" %(SGT, Row['price'], Row['side'], Row['size']))
            else:
                continue
            conn.commit()

        finally:
            conn.close()

        del SGT



def run():

    # Instantiating the WS will make it connect. Be sure to add your api_key/api_secret.



    ws = BitMEXWebsocket(endpoint="https://www.bitmex.com/api/v1", symbol="XBTUSD", api_key='',
                         api_secret='')
    # Run forever

    while(True):

        try:
            Recent_Trade = ws.recent_trades()
            export_to_MySQL(Recent_Trade)

            if not ws.ws.sock.connected:
                ws.ws.run_forever()

        except AttributeError as ae:
            ws.ws.run_forever()





        sleep(10)


if __name__ == "__main__":

    run()

