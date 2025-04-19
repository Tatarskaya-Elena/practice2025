INSERT OVERWRITE TABLE ds.ft_balance_f
SELECT * FROM ft_balance_f_temp;
INSERT OVERWRITE TABLE ds.md_account_d
SELECT * FROM md_account_d_temp;
INSERT OVERWRITE TABLE ds.md_currency
SELECT * FROM md_currency_d_temp;
