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

    md_account_d.write.format("hive").mode("append").saveAsTable("ds.md_account_d")

    md_currency_d= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_currency_d.csv"), header=True)
    md_currency_d = md_currency_d \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("data_actual_date", to_date(col("data_actual_date"))) \
        .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"))) \
        .withColumn("currency_code", col("currency_code").cast("string")) \
        .withColumn("code_iso_char", col("code_iso_char").cast("string")) \

    md_currency_d.write.format("hive").mode("append").saveAsTable("ds.md_currency_d")

    md_exchange_rate_d= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_exchange_rate_d.csv"), header=True)
    md_exchange_rate_d = md_exchange_rate_d \
        .withColumn("data_actual_date", to_date(col("data_actual_date"))) \
        .withColumn("data_actual_end_date", to_date(col("data_actual_end_date"))) \
        .withColumn("currency_rk", col("currency_rk").cast("decimal(38, 10)")) \
        .withColumn("reduced_cource", col("reduced_cource").cast("float")) \
        .withColumn("code_iso_num", col("code_iso_num").cast("string")) \

    md_exchange_rate_d.write.format("hive").mode("append").saveAsTable("ds.md_exchange_rate_d")

"""   md_ledger_account_s= spark.read.option("delimiter", ";").csv(os.path.join(DATA_PATH, "md_ledger_account_s.csv"), header=True)
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
        .withColumn("characteristic", col("characteristic").cast("int")) \
        .withColumn("is_resident", col("is_resident").cast("int")) \
        .withColumn("is_reserve", col("is_reserve").cast("int")) \
        .withColumn("is_reserved", col("is_reserved").cast("int")) \
        .withColumn("is_loan", col("is_loan").cast("int")) \
        .withColumn("is_reserved_assets", col("is_reserved_assets").cast("int")) \
        .withColumn("is_overdue", col("is_overdue").cast("int")) \
        .withColumn("is_interest", col("is_interest").cast("int")) \
        .withColumn("pair_account", col("pair_account").cast("string")) \
        .withColumn("start_date", to_date(col("start_date"))) \
        .withColumn("end_date", to_date(col("end_date"))) \
        .withColumn("is_rub_only", col("is_rub_only").cast("int")) \
        .withColumn("currency_code", col("currency_code").cast("string")) \
        .withColumn("min_term", col("min_term").cast("string")) \
        .withColumn("min_term_measure", col("min_term_measure").cast("string")) \
        .withColumn("max_term", col("max_term").cast("string")) \
        .withColumn("max_term_measure", col("max_term_measure").cast("string")) \
        .withColumn("ledger_acc_full_name_translit", col("ledger_acc_full_name_translit").cast("string")) \
        .withColumn("is_revaluation", col("is_revaluation").cast("string")) \
        .withColumn("is_correct", col("is_correct").cast("string"))
    md_ledger_account_s.write.format("hive").mode("append").saveAsTable("ds.md_ledger_account_s")
"""
if __name__ == "__main__":
    main()