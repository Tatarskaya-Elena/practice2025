import os

from datetime import datetime, timedelta

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
spark = get_spark_session(app_name="fill_dm")
spark.sparkContext.setCheckpointDir(os.path.join(SQL_PATH,"tmp"))

def fill_account_turnover_f(date):
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "fill_account_turnover_f.sql"), log_sql=True, on_date=date)
    dm_account_turnover_f=spark.read.table("dm.dm_account_turnover_f")
    dm_account_turnover_f_unique = dm_account_turnover_f.dropDuplicates().checkpoint()
    dm_account_turnover_f_unique.write.format("hive").mode("overwrite").insertInto("dm.dm_account_turnover_f")

def main():
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_showcase.sql"), log_sql=True)
    current_date = datetime(2018, 1, 1)
    while current_date <= datetime(2018, 1, 31):
        fill_account_turnover_f(current_date.date())
        current_date += timedelta(days=1)
if __name__ == "__main__":
    main()