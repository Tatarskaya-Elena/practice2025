CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    on_date DATE,
    account_rk DECIMAL(23,8),
    currency_rk DECIMAL(23,8),
    balance_out FLOAT
) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    oper_date DATE,
    credit_account_rk DECIMAL(23,8),
    debet_account_rk DECIMAL(23,8),
    credit_amount FLOAT,
    debet_amount FLOAT
) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS ds.md_account_d (
    data_actual_date DATE,
    data_actual_end_date DATE,
    account_rk DECIMAL(23,8),
    account_number STRING,
    char_type STRING,
    currency_rk DECIMAL(23,8),
    currency_code STRING
) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    currency_rk DECIMAL(23,8),
    data_actual_date DATE,
    data_actual_end_date DATE,
    currency_code STRING,
    code_iso_char STRING
) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    data_actual_date DATE,
    data_actual_end_date DATE,
    currency_rk DECIMAL(23,8),
    reduced_cource FLOAT,
    code_iso_num STRING
) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    chapter STRING,
    chapter_name STRING,
    section_number INT,
    section_name STRING,
    subsection_name STRING,
    ledger1_account INT,
    ledger1_account_name STRING,
    ledger_account INT,
    ledger_account_name STRING,
    characteristic STRING,
    is_resident INT,
    is_reserve INT,
    is_reserved INT,
    is_loan INT,
    is_reserved_assets INT,
    is_overdue INT,
    is_interest INT,
    pair_account STRING,
    start_date DATE,
    end_date DATE,
    is_rub_only INT,
    min_term STRING,
    min_term_measure STRING,
    max_term STRING,
    max_term_measure STRING,
    ledger_acc_full_name_translit STRING,
    is_revaluation STRING,
    is_correct STRING
) STORED AS PARQUET;