CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f(
<<<<<<< Updated upstream:etl/flows/dm/fill_dm/sql/create_showcase.sql
    on_date DATE,
    account_rk DECIMAL,
    credit_amount DECIMAL,
    credit_amount_rub DECIMAL,
    debet_amount DECIMAL,
    debet_amount_rub DECIMAL
) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f(
    on_date DATE,
    account_rk DECIMAL,
    balance_out DECIMAL,
    balance_out_rub DECIMAL
) STORED AS PARQUET;
=======
    account_rk DECIMAL(23,8),
    credit_amount DECIMAL(23,8),
    credit_amount_rub DECIMAL(23,8),
    debet_amount DECIMAL(23,8),
    debet_amount_rub DECIMAL(23,8)
) PARTITIONED BY (on_date DATE) STORED AS PARQUET;
CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f(
    account_rk DECIMAL(38,8),
    balance_out DECIMAL(38,8),
    balance_out_rub DECIMAL(38,8)
) PARTITIONED BY (on_date DATE) STORED AS PARQUET;
>>>>>>> Stashed changes:etl/flows/dm/fill_dm_account_turnover_f/sql/create_showcase.sql
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f(
    FROM_DATE DATE,
    TO_DATE DATE,
    CHAPTER STRING,
    LEDGER_ACCOUNT STRING,
    CHARACTERISTIC STRING,
    BALANCE_IN_RUB DECIMAL,
    R_BALANCE_IN_RUB DECIMAL,
    BALANCE_IN_VAL DECIMAL,
    R_BALANCE_IN_VAL DECIMAL,
    BALANCE_IN_TOTAL DECIMAL,
    R_BALANCE_IN_TOTAL DECIMAL,
    TURN_DEB_RUB DECIMAL,
    R_TURN_DEB_RUB DECIMAL,
    TURN_DEB_VAL DECIMAL,
    R_TURN_DEB_VAL DECIMAL,
    TURN_DEB_TOTAL DECIMAL,
    R_TURN_DEB_TOTAL DECIMAL,
    TURN_CRE_RUB DECIMAL,
    R_TURN_CRE_RUB DECIMAL,
    TURN_CRE_VAL DECIMAL,
    R_TURN_CRE_VAL DECIMAL,
    TURN_CRE_TOTAL DECIMAL,
    R_TURN_CRE_TOTAL DECIMAL,
    BALANCE_OUT_RUB DECIMAL,
    R_BALANCE_OUT_RUB DECIMAL,
    BALANCE_OUT_VAL DECIMAL,
    R_BALANCE_OUT_VAL DECIMAL,
    BALANCE_OUT_TOTAL DECIMAL,
    R_BALANCE_OUT_TOTAL DECIMAL
) STORED AS PARQUET;