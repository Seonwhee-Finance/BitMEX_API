import sys
import logging
import rds_config
import pymysql,json
from datetime import datetime


#rds settings
rds_host  = ""
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
#s3_client = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    This function fetches content from MySQL RDS instance
    """

    Query = "INSERT INTO model_output_%s(pred_time, model_name, pred, steady_percent) VALUES (STR_TO_DATE(\'%s\',\' %s\'),\'%s\', %d, %.2f);" % (
    event['after'], event['pred_time'], "%Y-%m-%d %H:%i:%s", event['model_name'], event['pred'], event["steady"])
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)

    try:
        curs = conn.cursor()
        logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
        print(Query)
        curs.execute(Query)
        conn.commit()

    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        sys.exit()

    finally:
        conn.close()



