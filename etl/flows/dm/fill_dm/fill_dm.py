import os

from datetime import datetime

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")

def main():
    spark = get_spark_session(app_name="fill_ds")
    spark.sparkContext.setCheckpointDir(os.path.join(SQL_PATH,"tmp"))

    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_showcase.sql"), log_sql=True)

if __name__ == "__main__":
    main()