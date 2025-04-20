INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
SELECT 
    TO_DATE('{on_date}') AS on_date,
    account_rk,
    SUM(CASE WHEN ds.ft_posting_f.credit_account_rk = account_rk THEN ds.ft_posting_f.credit_amount ELSE 0 END) AS credit_amount,
    SUM(CASE 
            WHEN ds.ft_posting_f.credit_account_rk = account_rk THEN 
                ds.ft_posting_f.credit_amount * COALESCE(exchange.reduced_cource, 1) 
            ELSE 0 
        END) AS credit_amount_rub,
    SUM(CASE WHEN ds.ft_posting_f.debet_account_rk = account_rk THEN ds.ft_posting_f.debet_amount ELSE 0 END) AS debet_amount,
    SUM(CASE 
            WHEN ds.ft_posting_f.debet_account_rk = account_rk THEN 
                ds.ft_posting_f.debet_amount * COALESCE(exchange.reduced_cource, 1) 
            ELSE 0 
        END) AS debet_amount_rub
FROM 
    ds.ft_posting_f
JOIN 
    ds.md_account_d ON ds.md_account_d.account_rk IN (ds.ft_posting_f.credit_account_rk, ds.ft_posting_f.debet_account_rk)
LEFT JOIN 
    (SELECT 
         currency_rk, 
         reduced_cource 
     FROM 
         ds.md_exchange_rate_d 
     WHERE 
         data_actual_date < TO_DATE('{on_date}') AND 
         data_actual_end_date > TO_DATE('{on_date}')
    ) exchange ON exchange.currency_rk = ds.md_account_d.currency_rk
WHERE 
    ds.ft_posting_f.oper_date = TO_DATE('{on_date}')
GROUP BY 
    account_rk
HAVING 
    SUM(CASE WHEN ds.ft_posting_f.credit_account_rk = account_rk THEN ds.ft_posting_f.credit_amount ELSE 0 END) > 0 OR
    SUM(CASE WHEN ds.ft_posting_f.debet_account_rk = account_rk THEN ds.ft_posting_f.debet_amount ELSE 0 END) > 0;