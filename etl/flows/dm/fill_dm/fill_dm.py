import os
import time

from datetime import datetime, timedelta

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
spark = get_spark_session(app_name="fill_dm")
spark.sparkContext.setCheckpointDir(os.path.join(SQL_PATH,"tmp"))

def fill_account_turnover_f(date):
    df = spark.table("dm.dm_account_turnover_f")
    filtered_df = df.filter(col("on_date") != date).checkpoint()
    filtered_df.write.mode('overwrite').saveAsTable("dm.dm_account_turnover_f")
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "fill_account_turnover_f.sql"), log_sql=True, on_date=date)

def fill_account_balance_f(date):
    df = spark.table("dm.dm_account_balance_f")
    filtered_df = df.filter(col("on_date") != date).checkpoint()
    filtered_df.write.mode('overwrite').saveAsTable("dm.dm_account_balance_f")
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "fill_account_balance_f.sql"), log_sql=True, on_date=date)

def main():

    fal=False
    start_time = datetime.now() 
    try:
        sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_showcase.sql"), log_sql=True)
        current_date = datetime(2018, 1, 1)
        while current_date <= datetime(2018, 1, 31):
            fill_account_turnover_f(current_date.date())
            current_date += timedelta(days=1)
    except Exception as e:
        fal = True
    finally:
        end_time = datetime.now() 
        log_process_start_end(spark,"fill_account_turnover_f", start_time, end_time, fal)

    fal=False
    start_time = datetime.now() 
    try:
        dm_account_balance_f = spark.table("dm.dm_account_balance_f")
        filtered_dm_account_balance_f = dm_account_balance_f.filter(col("on_date") != '2017-12-31').checkpoint()
        filtered_dm_account_balance_f.write.mode('overwrite').saveAsTable("dm.dm_account_balance_f")
        sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "insert_balance.sql"), log_sql=True)
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