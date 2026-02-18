create or replace view CARRY1ST_PLATFORM.REFINED.PURCHASE_ATTEMPT as 

WITH base AS (
    SELECT 
        distinct
        email,
        reference,
        trx_datetime,  -- assuming you have this column for the timestamp
        IFF(classification IN ('Success', 'Suspicious Successful'), 1, 0) AS success_flag
    FROM carry1st_platform.refined.transaction_detail_s
    WHERE 1=1
        AND trx_date <= CURRENT_DATE() - 1
        AND payment_gateway IS NOT NULL
        AND final_status IS NOT NULL
),

with_lags AS (
    SELECT *,
        LAG(trx_datetime) OVER (PARTITION BY email ORDER BY trx_datetime) AS prev_dt,
        LAG(success_flag) OVER (PARTITION BY email ORDER BY trx_datetime) AS prev_success
    FROM base
),

grouped AS (
    SELECT *,
        CASE 
            WHEN prev_dt IS NULL THEN 1  -- first trx
            WHEN prev_success = 1 THEN 1 -- previous was a success, start new group
            WHEN DATEDIFF('second', prev_dt, trx_datetime) > 480 THEN 1  -- gap > 480s
            ELSE 0
        END AS new_group
    FROM with_lags
),

assigned AS (
    SELECT *,
        SUM(new_group) OVER (PARTITION BY email ORDER BY trx_datetime ROWS UNBOUNDED PRECEDING) AS purchase_reference
    FROM grouped
),

final AS (
    SELECT *,
        MAX(success_flag) OVER (PARTITION BY email, purchase_reference) AS purchase_successful
    FROM assigned
)

SELECT 
    email,
    reference,
    trx_datetime,
    success_flag,
    purchase_reference,
    purchase_successful
FROM final
where 1=1;