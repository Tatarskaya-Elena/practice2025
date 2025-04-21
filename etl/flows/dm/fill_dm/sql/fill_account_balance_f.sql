INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT 
    TO_DATE('{on_date}') AS on_date,
    d.account_rk,
    COALESCE(b.balance_out, 0) + 
        CASE 
            WHEN d.char_type = 'А' THEN COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN d.char_type = 'П' THEN -COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
    COALESCE(b.balance_out_rub, 0) + 
        CASE 
            WHEN d.char_type = 'А' THEN COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN d.char_type = 'П' THEN -COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
FROM ds.md_account_d d
LEFT JOIN dm.dm_account_balance_f b ON d.account_rk = b.account_rk AND b.on_date = DATE_SUB(TO_DATE('{on_date}'), 1)
LEFT JOIN (
    SELECT account_rk, SUM(debet_amount) AS debet_amount, SUM(credit_amount) AS credit_amount,
           SUM(debet_amount_rub) AS debet_amount_rub, SUM(credit_amount_rub) AS credit_amount_rub
    FROM dm.dm_account_turnover_f
    WHERE on_date = TO_DATE('{on_date}')
    GROUP BY account_rk
) t ON d.account_rk = t.account_rk
WHERE d.data_actual_date <= TO_DATE('{on_date}') AND d.data_actual_end_date >=TO_DATE('{on_date}');