import os
import time

from datetime import datetime, timedelta

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
spark = get_spark_session(app_name="fill_dm_account_balance_f")
spark.sparkContext.setCheckpointDir(os.path.join(SQL_PATH,"tmp"))

def fill_account_balance_f(date):
    result=sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "fill_account_balance_f.sql"), log_sql=True, on_date=date)[0]
    result.write.format("hive").mode("overwrite").insertInto("dm.dm_account_balance_f")
def main():

    fal=False
    start_time = datetime.now() 
    try:
        result=sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "insert_balance.sql"), log_sql=True)[0]
        result.write.format("hive").mode("overwrite").insertInto("dm.dm_account_balance_f")
        current_date = datetime(2018, 1, 1)
        while current_date <= datetime(2018, 1, 31):
            fill_account_balance_f(current_date.date())
            current_date += timedelta(days=1)
    except Exception as e:
        fal = True
    finally:
        end_time = datetime.now()
        log_process_start_end(spark,"fill_account_balance_f", start_time, end_time, fal)
if __name__ == "__main__":
    main()