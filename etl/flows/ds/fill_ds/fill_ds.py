import os
import time

from datetime import datetime

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
DATA_PATH = os.environ.get("DATA_PATH")



def main():
    spark = get_spark_session(app_name="fill_ds")

    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_databases.sql"), log_sql=True)
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_log_table.sql"), log_sql=True)

    start_time = datetime.now() 

    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_ds_tables.sql"), log_sql=True)
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "clear_tables.sql"), log_sql=True)

    time.sleep(5)
    ft_balance_f=spark.read.table("ds.ft_balance_f")
    ft_balance_f_new = spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "ft_balance_f.csv"), header=True)
    ft_balance_f_new = ft_balance_f_new \
        .withColumn("on_date",
            when(col("on_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("on_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("on_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("on_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("on_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("on_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("account_rk", col("account_rk").cast("decimal(38, 10)")) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("balance_out", col("balance_out").cast("float")) \
    
    ft_balance_f_new = ft_balance_f_new.filter(
    col("on_date").isNotNull() & 
    col("account_rk").isNotNull()
    )
    ft_balance_f_all=ft_balance_f.unionByName(ft_balance_f_new)
    ft_balance_f_unique = ft_balance_f_all.dropDuplicates(['on_date', 'account_rk'])
    #ft_balance_f_unique.createOrReplaceTempView("ft_balance_f_temp")
    ft_balance_f_unique.write.format("hive").mode("append").insertInto("ds.ft_balance_f")

    ft_posting_f = spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "ft_posting_f.csv"), header=True)
    ft_posting_f = ft_posting_f \
        .withColumn("oper_date",
            when(col("oper_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("oper_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("oper_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("oper_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("oper_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("oper_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("credit_account_rk", col("credit_account_rk").cast("decimal(38, 10)")) \
        .withColumn("debet_account_rk", col("debet_account_rk").cast("decimal(38, 10)")) \
        .withColumn("credit_amount", col("credit_amount").cast("float")) \
        .withColumn("debet_amount", col("debet_amount").cast("float")) \

    ft_posting_f = ft_posting_f.filter(
    col("oper_date").isNotNull() & 
    col("credit_account_rk").isNotNull() &
    col("debet_account_rk").isNotNull()
    )

    ft_posting_f.write.format("hive").mode("append").saveAsTable("ds.ft_posting_f")

    md_account_d=spark.read.table("ds.md_account_d")
    md_account_d_new= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_account_d.csv"), header=True)
    md_account_d_new = md_account_d_new \
        .withColumn("data_actual_date",
            when(col("data_actual_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("data_actual_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("data_actual_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
    .withColumn("data_actual_end_date",
            when(col("data_actual_end_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("data_actual_end_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("data_actual_end_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("account_rk", col("account_rk").cast("decimal(38, 10)")) \
        .withColumn("account_number", col("account_number").cast("string")) \
        .withColumn("char_type", col("char_type").cast("string")) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("currency_code", col("currency_code").cast("string")) \
        
    md_account_d_new = md_account_d_new.filter(
    col("data_actual_date").isNotNull() & 
    col("data_actual_end_date").isNotNull()&
    col("account_rk").isNotNull() & 
    col("account_number").isNotNull()&
    col("char_type").isNotNull() & 
    col("currency_rk").isNotNull()&
    col("currency_code").isNotNull()
    )

    md_account_d_new = md_account_d_new.filter(
    (length(col("account_number")) <= 20) &
    (length(col("char_type")) <= 1) &
    (length(col("currency_code")) <= 3)
    )
    md_account_d_all=md_account_d.unionByName(md_account_d_new)
    md_account_d_unique =  md_account_d_all.dropDuplicates(['data_actual_date', 'account_rk'])
    md_account_d_unique.createOrReplaceTempView("md_account_d_temp")

    md_currency_d=spark.read.table("ds.md_currency_d")
    md_currency_d_new= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_currency_d.csv"), header=True)
    md_currency_d_new = md_currency_d_new \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("data_actual_date",
            when(col("data_actual_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("data_actual_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("data_actual_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("data_actual_end_date",
            when(col("data_actual_end_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("data_actual_end_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("data_actual_end_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("currency_code", col("currency_code").cast("string")) \
        .withColumn("code_iso_char", col("code_iso_char").cast("string")) \
        
    md_currency_d_new = md_currency_d_new.filter(
    col("currency_rk").isNotNull() & 
    col("data_actual_date").isNotNull()
    )

    md_currency_d_new = md_currency_d_new.filter(
    (length(col("currency_code")) <= 3) &
    (length(col("code_iso_char")) <= 3)
    )
    md_currency_d_all=md_currency_d.unionByName(md_currency_d_new)
    md_currency_d_unique =  md_currency_d_all.dropDuplicates(['currency_rk','data_actual_date'])
    md_currency_d_unique.createOrReplaceTempView("md_currency_d_temp")

    md_exchange_rate_d= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_exchange_rate_d.csv"), header=True)
    md_exchange_rate_d = md_exchange_rate_d \
          .withColumn("data_actual_date",
            when(col("data_actual_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("data_actual_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("data_actual_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("data_actual_end_date",
            when(col("data_actual_end_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("data_actual_end_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("data_actual_end_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("data_actual_end_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("reduced_cource", col("reduced_cource").cast("float")) \
        .withColumn("code_iso_num", col("code_iso_num").cast("string")) \
 
    md_exchange_rate_d = md_exchange_rate_d.filter(
    col("data_actual_date").isNotNull() &
    col("currency_rk").isNotNull()
    )

    md_exchange_rate_d = md_exchange_rate_d.filter(
    (length(col("code_iso_num")) <= 3)
    )
    
    md_exchange_rate_d_unique = md_exchange_rate_d.dropDuplicates(['data_actual_date','currency_rk'])
    md_exchange_rate_d_unique.write.format("hive").mode("append").saveAsTable("ds.md_exchange_rate_d")

    md_ledger_account_s= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_ledger_account_s.csv"), header=True)
    md_ledger_account_s= md_ledger_account_s \
        .withColumn("chapter", col("chapter").cast("string")) \
        .withColumn("chapter_name", col("chapter_name").cast("string")) \
        .withColumn("section_number", col("section_number").cast("int")) \
        .withColumn("section_name", col("section_name").cast("string")) \
        .withColumn("subsection_name", col("subsection_name").cast("string")) \
        .withColumn("ledger1_account", col("ledger1_account").cast("int")) \
        .withColumn("ledger1_account_name", col("ledger1_account_name").cast("string")) \
        .withColumn("ledger_account", col("ledger_account").cast("int")) \
        .withColumn("ledger_account_name", col("ledger_account_name").cast("string")) \
        .withColumn("characteristic", col("characteristic").cast("string")) \
        .withColumn("is_resident",lit(0)) \
        .withColumn("is_reserve", lit(0)) \
        .withColumn("is_reserved", lit(0)) \
        .withColumn("is_loan", lit(0)) \
        .withColumn("is_reserved_assets",lit(0)) \
        .withColumn("is_overdue",lit(0)) \
        .withColumn("is_interest", lit(0)) \
        .withColumn("pair_account", lit("")) \
        .withColumn("start_date",
            when(col("start_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("start_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("start_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("start_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("start_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("start_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("end_date",
            when(col("end_date").rlike(r'^\d{2}-\d{2}-\d{4}$'), 
            to_date(unix_timestamp(col("end_date"), 'dd-MM-yyyy').cast('timestamp')))
            .when(col("end_date").rlike(r'^\d{4}-\d{2}-\d{2}$'), 
            to_date(unix_timestamp(col("end_date"), 'yyyy-MM-dd').cast('timestamp')))
            .when(col("end_date").rlike(r'^\d{2}\.\d{2}\.\d{4}$'), 
            to_date(unix_timestamp(col("end_date"), 'dd.MM.yyyy').cast('timestamp')))
         .otherwise(None)
    ) \
        .withColumn("is_rub_only", lit(0)) \
        .withColumn("min_term", lit("")) \
        .withColumn("min_term_measure", lit("")) \
        .withColumn("max_term", lit("")) \
        .withColumn("max_term_measure", lit("")) \
        .withColumn("ledger_acc_full_name_translit", lit("")) \
        .withColumn("is_revaluation", lit("")) \
        .withColumn("is_correct", lit(""))
    
    md_ledger_account_s = md_ledger_account_s.filter(
    col("ledger_account").isNotNull() & 
    col("start_date").isNotNull()
    )

    md_ledger_account_s = md_ledger_account_s.filter(
    (length(col("chapter")) == 1) &
    (length(col("chapter_name")) <= 16) &
    (length(col("section_name")) <= 22) &
    (length(col("subsection_name")) <= 21) &
    (length(col("ledger1_account_name")) <= 47) &
    (length(col("ledger_account_name")) <= 153) &
    (length(col("characteristic")) == 1) &
    (length(col("pair_account")) <= 5) &
    (length(col("min_term")) <= 1) &
    (length(col("min_term_measure")) <= 1) &
    (length(col("max_term")) <= 1) &
    (length(col("max_term_measure")) <= 1) &
    (length(col("ledger_acc_full_name_translit")) <= 1)&
    (length(col("is_revaluation")) <= 1) &
    (length(col("is_correct")) <= 1)
    )

    md_ledger_account_s_unique = md_ledger_account_s.dropDuplicates(['ledger_account','start_date'])
    md_ledger_account_s_unique.write.format("hive").mode("append").saveAsTable("ds.md_ledger_account_s")

    end_time = datetime.now() 
    log_process_start_end(spark,"fill_ds", start_time, end_time)
    #sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "overwrite_tables.sql"), log_sql=True)
    spark.sql('SELECT * FROM log.data_load_logs').show()

if __name__ == "__main__":
    main()