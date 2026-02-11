CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.refined.CAMPFIRE__CREDIT_VS_DEBIT AS

WITH trx_cte AS (
    SELECT    
        LAST_DAY(trx_date) AS last_day_month,
        provider_name,
        ROUND(SUM(
            CASE 
                WHEN provider_name = 'Activision' THEN fiat_processed_amount_local * 0.1
                WHEN  provider_name = 'Audiomack' THEN fiat_processed_amount_local * 0.15
                WHEN  provider_name = 'Boomplay' THEN fiat_processed_amount_local * 0.3
                WHEN  provider_name = 'EA Sports' THEN fiat_processed_amount_local * 0.15
                WHEN  provider_name = 'MPL' THEN fiat_processed_amount_local * 0.02 + psp_fee_local
                WHEN  provider_name = 'Nanobit' THEN fiat_processed_amount_local * 0.15
                WHEN  provider_name = 'Riot' THEN fiat_processed_amount_local * 0.035 + psp_fee_local
                WHEN  provider_name = 'Timwe Group' THEN fiat_processed_amount_local * 0.1
                ELSE 0
                END)) AS p1st_comission,
        ROUND(SUM(fiat_processed_amount_local),2) AS processed_amount,
        ROUND(SUM(psp_fee_local),2) AS psp_fee
    FROM carry1st_platform.refined.transaction_detail_s AS s
    WHERE 1=1
        AND reporting_flag = 'Yes'
        AND trx_date >= '2024-01-01'
        AND tech_gate_source IN ('MPL','RIOT','SHOP')
        AND payment_gateway = 'Paystack'
        AND to_char(trx_date, 'YYYY-MM') >= '2025-11'
    GROUP BY 1,2
),

map_cte AS (
    SELECT 
        provider,
        entity,
        revenue_types,
        metric,
        account,
        account_type,
        calc,
        ACCOUNT_NAME,
        cost_center,
        games,
        retail,
        description, 
        reference
    FROM CARRY1ST_PLATFORM.refined.UPLOAD__CAMPFIRE 
),

join_cte AS (
    SELECT 
        t.last_day_month,
        m.provider,
        m.entity,
        m.revenue_types,
        m.account,
        m.account_type,
        m.ACCOUNT_NAME,
        m.cost_center,
        m.games,
        m.retail,
        REPLACE(m.description, '__month_year__', TO_CHAR(t.last_day_month, 'MMMM YY')) AS description, 
        m.reference,
        m.calc,
        CASE 
            WHEN calc = 'P1ST_COMISSION' THEN t.p1st_comission
            WHEN calc = 'PROCESSED_AMT' THEN t.processed_amount
            WHEN calc = 'PROCESSED_AMT - P1ST_COMISSION' THEN t.processed_amount - t.p1st_comission
            WHEN calc = 'PSP_FEE' THEN t.psp_fee
            ELSE 0
            END AS value 
    FROM map_cte AS m
    LEFT JOIN trx_cte AS t
        ON  m.provider = t.provider_name
        AND m.metric NOT IN ('ADG & SA SUM', 'HBI PULL', 'HBI SUM') 
    WHERE 1=1
       -- AND t.last_day_month = '2025-11-30'
),

union_cte AS (
    SELECT 
        j.last_day_month,
        m.provider AS provider,
        m.entity AS entity,
        m.revenue_types ,
        m.account AS account,
        CASE 
            WHEN j.account_type = 'DEBIT' THEN 'CREDIT'
            WHEN j.account_type = 'CREDIT' THEN 'DEBIT'
            END AS account_type,
        m.account_name,
        m.cost_center,
        m.games,
        m.retail,
        REPLACE(m.description, '__month_year__', TO_CHAR(j.last_day_month, 'MMMM YY')) AS description, 
        m.reference,
        SUM(j.value) AS value 
    FROM join_cte AS j
    LEFT JOIN map_cte AS m
        ON m.metric = 'HBI SUM'
      --  AND gateway = gateway
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

    UNION ALL 

    SELECT 
        j.last_day_month,
        m.provider AS provider,
        j.entity,
        m.revenue_types AS revenue_types,
        m.account AS account,
        CASE 
            WHEN m.account_type = 'REVERSE' AND j.account_type = 'DEBIT' THEN 'CREDIT'
            WHEN m.account_type = 'REVERSE' AND j.account_type = 'CREDIT' THEN 'DEBIT'
            ELSE j.account_type
            END AS account_type,
        m.account_name,
        m.cost_center,
        m.games,
        m.retail,
        REPLACE(m.description, '__month_year__', TO_CHAR(j.last_day_month, 'MMMM YY')) AS description, 
        m.reference,
        SUM(value) AS value 
    FROM join_cte AS j
    INNER JOIN map_cte AS m
        ON j.entity = m.entity
        AND m.metric = 'HBI PULL'
      --  AND gateway = gateway
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12

    UNION ALL

    SELECT 
        j.last_day_month,
        m.provider,
        m.entity,
        m.revenue_types AS revenue_types,
        m.account AS account,
        j.account_type AS account_type,
        m.account_name,
        m.cost_center,
        m.games,
        m.retail,
        REPLACE(m.description, '__month_year__', TO_CHAR(j.last_day_month, 'MMMM YY')) AS description, 
        m.reference,
        SUM(value) AS value 
    FROM join_cte AS j
    INNER JOIN map_cte AS m
        ON j.entity = m.provider
        AND m.metric = 'ADG & SA SUM'
      --  AND gateway = gateway
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12


    UNION ALL

    SELECT
        last_day_month,
        provider,
        entity,
        revenue_types,
        account,
        account_type,
        account_name,
        cost_center,
        games,
        retail,
        description, 
        reference,
        value 
    FROM join_cte
)

SELECT
        last_day_month,
        entity,
        account,
        account_name,
        cost_center,
        games,
        retail,
        description, 
        reference,
        'Journal Entry' As upload_type,
        'NGN' AS currency,
        'TRUE' AS monthly_avg_rate,
        IFF(account_type = 'DEBIT', value, 0) AS debit,
        IFF(account_type = 'CREDIT', value, 0) AS credit,
FROM union_cte 
WHERE 1=1
    AND value <> 0