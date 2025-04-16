CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    on_date DATE,
    account_rk DECIMAL(38, 0),
    currency_rk DECIMAL(38, 0),
    balance_out FLOAT
) STORED AS PARQUET;