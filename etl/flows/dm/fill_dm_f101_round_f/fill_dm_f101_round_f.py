import os
import time

from datetime import datetime, timedelta

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
spark = get_spark_session(app_name="fill_dm_f101_round_f")
spark.sparkContext.setCheckpointDir(os.path.join(SQL_PATH,"tmp"))

def fill_f101_round_f(date):
    result=sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "fill_f101_round_f.sql"), log_sql=True, date=date)[0]
    result.write.format("hive").mode("overwrite").insertInto("dm.dm_f101_round_f")
def main():

    fal=False
    start_time = datetime.now() 
    try:
        current_date = datetime(2018, 2, 1)
        fill_f101_round_f(current_date.date())
    except Exception as e:
        fal = True
    finally:
        end_time = datetime.now()
        log_process_start_end(spark,"fill_f101_round_f", start_time, end_time, fal)
if __name__ == "__main__":
    main()