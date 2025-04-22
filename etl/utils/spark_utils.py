from pyspark.sql import SparkSession
import logging


def get_spark_session(app_name: str = "App") -> SparkSession:
    return SparkSession.builder.master("local[*]").enableHiveSupport().config("spark.sql.source.partitionOverwriteMode","dynamic").getOrCreate()


def sql_from_file(spark: SparkSession, file_path: str, log_sql: bool = False, **kwargs):
    sql = open(file_path).read().format(**kwargs).split(";")
    result=[]
    for sentance in sql:
        sentance = sentance.strip()

        if log_sql:
            logging.warning(sentance)
            
        if sentance:
            result.append(spark.sql(sentance))
    return result
        

def log_process_start_end(spark, names, start_time, end_time,fal):
    log_data = [(names, start_time, end_time, fal)]
    log_df = spark.createDataFrame(log_data, ["names","start_time", "end_time", "fal"])
    log_df.write.format("hive").mode("append").saveAsTable("log.data_load_logs")