from pyspark.sql import SparkSession
import logging


def get_spark_session(app_name: str = "App") -> SparkSession:
    return SparkSession.builder.master("local[*]").enableHiveSupport().getOrCreate()


def sql_from_file(spark: SparkSession, file_path: str, log_sql: bool = False, **kwargs):
    sql = open(file_path).read().format(**kwargs).split(";")

    for sentance in sql:
        sentance = sentance.strip()

        if log_sql:
            logging.info(sentance)
            
        if sentance:
            spark.sql(sentance)

def log_process_start_end(spark, names, start_time, end_time):
    log_data = [(names, start_time, end_time)]
    log_df = spark.createDataFrame(log_data, ["names","start_time", "end_time"])
    log_df.write.format("hive").mode("append").saveAsTable("log.data_load_logs")