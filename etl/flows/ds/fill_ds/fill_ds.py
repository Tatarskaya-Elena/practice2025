import os

from etl.utils.spark_utils import *
from pyspark.sql.functions import *

SQL_PATH = os.path.join(os.path.dirname(__file__), "sql")
DATA_PATH = os.environ.get("DATA_PATH")


def main():
    spark = get_spark_session(app_name="fill_ds")

    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_databases.sql"), log_sql=True)
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "create_ds_tables.sql"), log_sql=True)
    sql_from_file(spark=spark, file_path=os.path.join(SQL_PATH, "clear_tables.sql"), log_sql=True)

    ft_balance_f = spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "ft_balance_f.csv"), header=True)
    ft_balance_f = ft_balance_f \
        .withColumn("on_date", to_date(col("on_date"))) \
        .withColumn("account_rk", col("account_rk").cast("decimal(38, 10)")) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("balance_out", col("balance_out").cast("float")) \

    ft_balance_f_unique = ft_balance_f.dropDuplicates(['on_date', 'account_rk'])
    ft_balance_f_unique.write.format("hive").mode("append").saveAsTable("ds.ft_balance_f")

    ft_posting_f = spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "ft_posting_f.csv"), header=True)
    ft_posting_f = ft_posting_f \
        .withColumn("oper_date", to_date(col("oper_date"))) \
        .withColumn("credit_account_rk", col("credit_account_rk").cast("decimal(38, 10)")) \
        .withColumn("debet_account_rk", col("debet_account_rk").cast("decimal(38, 10)")) \
        .withColumn("credit_amount", col("credit_amount").cast("float")) \
        .withColumn("debet_amount", col("debet_amount").cast("float")) \

    ft_posting_f.write.format("hive").mode("append").saveAsTable("ds.ft_posting_f")
    
    md_account_d= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_account_d.csv"), header=True)
    md_account_d = md_account_d \
        .withColumn("data_actual_date", to_date(col("data_actual_date"))) \
        .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"))) \
        .withColumn("account_rk", col("account_rk").cast("decimal(38, 10)")) \
        .withColumn("account_number", col("account_number").cast("string")) \
        .withColumn("char_type", col("char_type").cast("string")) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("currency_code", col("currency_code").cast("string")) \

    md_account_d_unique =  md_account_d.dropDuplicates(['data_actual_date', 'account_rk'])
    md_account_d.write.format("hive").mode("append").saveAsTable("ds.md_account_d")

    md_currency_d= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_currency_d.csv"), header=True)
    md_currency_d = md_currency_d \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("data_actual_date", to_date(col("data_actual_date"))) \
        .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"))) \
        .withColumn("currency_code", col("currency_code").cast("string")) \
        .withColumn("code_iso_char", col("code_iso_char").cast("string")) \

    md_currency_d_unique =  md_currency_d.dropDuplicates(['currency_rk','data_actual_date'])
    md_currency_d_unique.write.format("hive").mode("append").saveAsTable("ds.md_currency_d")

    md_exchange_rate_d= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_exchange_rate_d.csv"), header=True)
    md_exchange_rate_d = md_exchange_rate_d \
        .withColumn("data_actual_date", to_date(col("data_actual_date"))) \
        .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"))) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("reduced_cource", col("reduced_cource").cast("float")) \
        .withColumn("code_iso_num", col("code_iso_num").cast("string")) \

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
        .withColumn("start_date", to_date(col("start_date"))) \
        .withColumn("end_date", to_date(col("end_date"))) \
        .withColumn("is_rub_only", lit(0)) \
        .withColumn("currency_code", lit("")) \
        .withColumn("min_term", lit("")) \
        .withColumn("min_term_measure", lit("")) \
        .withColumn("max_term", lit("")) \
        .withColumn("max_term_measure", lit("")) \
        .withColumn("ledger_acc_full_name_translit", lit("")) \
        .withColumn("is_revaluation", lit("")) \
        .withColumn("is_correct", lit(""))
    
    md_ledger_account_s_unique = md_ledger_account_s.dropDuplicates(['ledger_account','start_date'])
    md_ledger_account_s_unique.write.format("hive").mode("append").saveAsTable("ds.md_ledger_account_s")
if __name__ == "__main__":
    main()