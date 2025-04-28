import os
import time

from datetime import datetime, timedelta

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
spark = get_spark_session(app_name="download_form_101_to_csv")
spark.sparkContext.setCheckpointDir(os.path.join(SQL_PATH,"tmp"))


def download_to_csv(table_name, path):
    df = spark.table(table_name)
    df.write.csv(path, header=True, mode="overwrite")

def main():

    fal=False
    start_time = datetime.now() 
    try:
        table_name = "dm.dm_f101_round_f" 
        path = "/home/misha/practice2025/practice2025/data/dm_f101_round_f.csv"

        download_to_csv(table_name, path)

        df_new = spark.read.option("delimiter", ",").csv(os.path.join(path, "*.csv"), header=True)
        df_new = df_new.withColumn("CHARACTERISTIC", when(col("CHARACTERISTIC") == "П", "P").otherwise(col("CHARACTERISTIC")))
        path = "/home/misha/practice2025/practice2025/data/dm_f101_round_f_copy.csv"
        df_new.write.csv(path, header=True, mode="overwrite")
    except Exception as e:  
       fal = True
    finally:
        end_time = datetime.now() 
        log_process_start_end(spark,"download_form_101_to_csv", start_time, end_time, fal)
if __name__ == "__main__":
    main()