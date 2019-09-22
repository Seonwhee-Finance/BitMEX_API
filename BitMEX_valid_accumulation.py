import pymysql, telegram
import schedule
from time import monotonic, sleep
from datetime import datetime, timedelta
from pytz import all_timezones, timezone, utc


def get_weekly_stats(pred_minute):
    now = datetime.now()
    StartDay = now - timedelta(days=7)
    ToDay = now.date()
    StartDay = StartDay.date()

    Total_num, num_corr = getting_count(StartDay, ToDay, pred_minute)
    if Total_num == 0:
        msg = "지난 주간 누적 결과입니다. \n %s분 예측 결과 \n 총 %d 회" % (pred_minute, Total_num)
        msg_eng = "Weekly Stats \n Prediction before %sminutes \n Total %d predictions" % (
            pred_minute, Total_num)
    else:
        msg = "지난 주간 누적 결과입니다. \n %s분 예측 결과 \n 총 %d 회 \n 정답 %d 회 \n 적중율 %.2f" % (
            pred_minute, Total_num, num_corr, num_corr * 100 / Total_num)
        msg_eng = "Weekly Stats \n Prediction before %sminutes \n Total %d predictions \n Correct : %d  \n Accuracy : %.2f" % (
            pred_minute, Total_num, num_corr, num_corr * 100 / Total_num)

    # print(msg)
    Telegram_Alert(msg, pred_minute)



def get_daily_stats(pred_minute):
    now = datetime.now()
    # StartDay = now - timedelta(days=1)
    # ToDay = now.date()
    StartDay = now.date()
    ToDay = now + timedelta(days=1)
    ToDay = ToDay.date()

    Total_num, num_corr = getting_count(StartDay, ToDay, pred_minute)
    if Total_num == 0:
        msg = "%s 하루 누적 결과입니다. \n %s분 예측 결과 \n 총 %d 회" % (StartDay, pred_minute, Total_num)
        msg_eng = "%s Daily Stats \n Prediction before %sminutes \n Total %d predictions" % (
            StartDay, pred_minute, Total_num)
    else:
        msg = "%s 하루 누적 결과입니다. \n %s분 예측 결과 \n 총 %d 회 \n 정답 %d 회 \n 적중율 %.2f" % (
            StartDay, pred_minute, Total_num, num_corr, num_corr * 100 / Total_num)
        msg_eng = "%s Daily Stats \n Prediction before %sminutes \n Total %d predictions \n Correct : %d  \n Accuracy : %.2f" % (
            StartDay, pred_minute, Total_num, num_corr, num_corr * 100 / Total_num)

    # print(msg)
    Telegram_Alert(msg, pred_minute)
    Telegram_Alert_English(msg_eng, pred_minute)



def getting_count(StartDay, ToDay, pred_minute):

    Query_Correct = "SELECT COUNT(correct) FROM model_validation_%dmin WHERE pred_time>=\'%s\' AND pred_time<\'%s\' AND correct=\'TRUE\';" % (
    pred_minute, StartDay, ToDay)
    Query_Wrong = "SELECT COUNT(correct) FROM model_validation_%dmin WHERE pred_time>=\'%s\' AND pred_time<\'%s\' AND correct=\'FALSE\';" % (
        pred_minute, StartDay, ToDay)

    conn = pymysql.connect(host='', user='',
                           password='', db='', charset='utf8')
    try:
        curs = conn.cursor()
        curs.execute(Query_Correct)
        num_corr = curs.fetchone()[0]
        conn.commit()

        curs.execute(Query_Wrong)
        num_wrong = curs.fetchone()[0]
        conn.commit()

    finally:
        conn.close()

    Total_num = num_corr + num_wrong

    return Total_num, num_corr




def Telegram_Alert(msg, predTime):

    my_token = ''
    bot.sendMessage(chat_id='', text="[BitMEX Bot_%smin] %s" % (str(predTime), msg))  # -263380334
    #bot.sendMessage(chat_id='@neuralbc_signalTest', text="[New DB Manager Bot_%smin] %s" % (str(predTime), msg))  # -263380334
    #bot.sendMessage(chat_id=-284042811, text="[BitMEX Bot_%smin] %s" % (str(predTime), msg))  # -263380334
    bot.sendMessage(chat_id='', text="[BitMEX Bot_%smin] %s" % (str(predTime), msg))

def Telegram_Alert_English(msg, predTime):

    my_token = ''
    bot = telegram.Bot(token=my_token)

    bot.sendMessage(chat_id='', text="[BitMEX Bot_%smin] %s" % (str(predTime), msg))


def Weekly_task_manager():

    get_weekly_stats(10)
    get_weekly_stats(20)
    get_weekly_stats(30)


def Daily_task_manager():

    get_daily_stats(10)
    get_daily_stats(20)
    get_daily_stats(30)


if __name__=="__main__":
    #Weekly_task_manager()
    #Daily_task_manager()



    schedule.every().day.at("23:31").do(Daily_task_manager)
    # #schedule.every().monday.at("23:31").do(Weekly_task_manager)
    while True:
        schedule.run_pending()
        sleep(1)
