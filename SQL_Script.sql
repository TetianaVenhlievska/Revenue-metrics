WITH monthly_revenue AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date)::date AS payment_month,  -- перший день місяця
        SUM(revenue_amount_usd) AS total_revenue
    FROM games_payments
    GROUP BY user_id, payment_month
),
revenue_lag_lead_months AS (
    SELECT
        mr.*,
        -- Календарний попередній і наступний місяць
        (payment_month - INTERVAL '1 month')::date AS prev_calendar_month,
        (payment_month + INTERVAL '1 month')::date AS next_calendar_month,
  -- Попередній і наступний місяць, коли користувач реально платив
        LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS prev_payment_month,
        LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_payment_month,
        -- Дохід з попереднього місяця (якщо був платіж)
        LAG(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS prev_month_revenue
    FROM monthly_revenue mr
),
revenue_metrics AS (
    SELECT
        payment_month,
        user_id,
        total_revenue,
        prev_calendar_month,
        next_calendar_month,
        prev_payment_month,
        next_payment_month,
        prev_month_revenue,
        -- New MRR: користувач не платив у попередньому місяці
        CASE 
            WHEN prev_payment_month IS NULL 
                 OR prev_payment_month <> prev_calendar_month 
            THEN total_revenue 
            ELSE 0 
        END AS new_mrr,
        -- Expansion: дохід зріс
        CASE 
            WHEN prev_payment_month = prev_calendar_month 
                 AND total_revenue > prev_month_revenue 
            THEN total_revenue - prev_month_revenue
            ELSE 0
        END AS expansion_revenue,
        -- Contraction: дохід впав
        CASE 
            WHEN prev_payment_month = prev_calendar_month 
                 AND total_revenue < prev_month_revenue 
            THEN prev_month_revenue - total_revenue
            ELSE 0
        END AS contraction_revenue,
        -- Churn: користувач платив минулого місяця, але цього вже не буде у наступному
        CASE 
            WHEN next_payment_month IS NULL 
                 OR next_payment_month <> next_calendar_month 
            THEN total_revenue
            ELSE 0
        END AS churned_revenue,
         CASE 
            WHEN next_payment_month IS NULL 
                 OR next_payment_month <> next_calendar_month 
            THEN 1
        END AS churned_users,
        -- Місяць, коли користувач "відвалився"
        CASE 
            WHEN next_payment_month IS NULL 
                 OR next_payment_month <> next_calendar_month 
            THEN payment_month
            ELSE NULL
        END AS churn_month
    FROM revenue_lag_lead_months)
SELECT  rm.payment_month,
        rm.user_id,
        rm.total_revenue,
        rm.prev_calendar_month,
        rm.next_calendar_month,
        rm.prev_payment_month,
        rm.next_payment_month,
        rm.prev_month_revenue,
        rm.new_mrr,
        rm.expansion_revenue,	
        rm.contraction_revenue,
        rm.churned_revenue,
        rm.churned_users, 
        rm.churn_month,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model 
FROM revenue_metrics rm
left join games_paid_users gpu 
on rm.user_id = gpu.user_id;
