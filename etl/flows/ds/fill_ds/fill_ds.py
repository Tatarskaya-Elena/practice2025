import os

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
DATA_PATH = os.environ.get("DATA_PATH")


def main():
    spark = get_spark_session(app_name="fill_ds")

    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_databases.sql"), log_sql=True)
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_ds_tables.sql"), log_sql=True)

    ft_balance_f = spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "ft_balance_f.csv"), header=True)
    ft_balance_f = ft_balance_f \
        .withColumn("on_date", to_date(col("on_date"))) \
        .withColumn("account_rk", col("account_rk").cast("decimal(38, 10)")) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("balance_out", col("balance_out").cast("float")) \

    ft_balance_f.write.format("hive").mode("append").saveAsTable("ds.ft_balance_f")


if __name__ == "__main__":
    main()