import pymysql, telegram, requests


def do():
    query = "SELECT * FROM price_one_hour WHERE pred_time>'2019-03-19 08:00:00' ORDER BY pred_time;"
    conn = pymysql.connect(host='localhost', user='root', password='neuralbc', db='trading', charset='utf8',
                           unix_socket="/var/run/mysqld/mysqld.sock")

    try:
        curs = conn.cursor()
        curs.execute(query)
        DB_schema2 = []
        cols = curs.fetchall()
        for col in cols:

            temp_record2 = [col[1].strftime('%Y-%m-%d %H:%M:%S'), col[4], col[2], col[5], col[3]]
            DB_schema2.append(temp_record2)
            del temp_record2
        print(DB_schema2)

        #chart_data = json.dumps(DB_schema, indent=2)



    finally:
        conn.close()


if __name__=="__main__":
    do()