CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.SINGULAR_USER_FIRST AS

WITH base_cte AS (
    SELECT 
        DISTINCT
        platform,
        session_date,
        session_datetime,
        singular_sid AS singular_full_sid,
        user AS singular_user_id,
        trx_id,
        final_source ||'||' || final_campaign AS source_campaign,
        IFF(
            (shop_user_id IS NOT NULL
                AND REGEXP_LIKE(shop_user_id, '^[0-9]+$') = true),
            shop_user_id,
            NULL) AS shop_user_id,
        country AS country_code,
    FROM CARRY1ST_PLATFORM.REFINED.SINGULAR_EVENT_DETAIL_S
    WHERE 1=1
        AND session_date <= SYSDATE()::date - 1  
),

first_session_cte AS (
    SELECT
        DISTINCT
        singular_user_id, 
        FIRST_VALUE(platform) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_platform,     
        FIRST_VALUE(source_campaign) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_session_source_campaign,     
        FIRST_VALUE(singular_full_sid) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_session,
        FIRST_VALUE(session_date) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_session_date,
        FIRST_VALUE(CASE WHEN trx_id IS NOT NULL THEN singular_full_sid ELSE NULL END) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_session_trx,
        FIRST_VALUE(CASE WHEN shop_user_id IS NOT NULL THEN singular_full_sid ELSE NULL END) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_account_session,
        FIRST_VALUE(CASE WHEN shop_user_id IS NOT NULL THEN session_date ELSE NULL END) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_account_date,
        FIRST_VALUE(country_code) IGNORE NULLS OVER(
            PARTITION BY singular_user_id 
            ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_country, 
    FROM base_cte 
),

trx_cte AS (
    SELECT
        trx_id,
        ROUND(SUM(gmv_n_gems), 2) AS gmv,
        ROUND(SUM(retail_nmv + pay1st_comission + service_fee + convinience_fee - chargeback_cost - cogs - psp_fee), 2) AS gp
    FROM CARRY1ST_PLATFORM.REFINED.TRANSACTION_DETAIL_S AS t
    WHERE 1=1
        AND t.gate_source = 'SHOP'
        AND t.reporting_flag = 'Yes'
    GROUP BY 1
),

singular_trx_cte AS (
    SELECT
        b.singular_user_id,
        b.trx_id,
        SUM(gmv) AS gmv,
        SUM(gp) AS gp,
        MIN(DATE(b.session_datetime)) AS trx_date        
    FROM base_cte AS b
    LEFT JOIN trx_cte AS t
        ON b.trx_id = t.trx_id
    WHERE 1=1
        AND b.trx_id IS NOT NULL
        AND t.trx_id IS NOT NULL
    GROUP BY 1, 2
),

first_trx_cte AS (
    SELECT
        singular_user_id,
        MIN(trx_date) AS first_trx_date
    FROM singular_trx_cte
    WHERE 1=1
    GROUP BY 1
),

ltv_cte AS (
    SELECT
        f.singular_user_id,
        f.first_trx_date AS first_session_trx_date,
        SUM(gmv) AS ltv_gmv,
        SUM(gp) AS ltv_gp
    FROM first_trx_cte AS f
    LEFT JOIN singular_trx_cte AS s
        ON f.singular_user_id = s.singular_user_id
        AND f.first_trx_date <= s.trx_date 
        AND DATEADD(month, 6, f.first_trx_date) >= s.trx_date
    GROUP BY 1, 2
),

final_cte AS (
    SELECT
        f.singular_user_id,
        f.first_session,
        f.first_session_date,
        f.first_session_source_campaign,
        f.first_session_trx,
        f.first_platform,
        f.first_account_session,
        f.first_account_date,
        f.first_country,
        l.first_session_trx_date,
        l.ltv_gmv,
        l.ltv_gp
    FROM first_session_cte AS f
    LEFT JOIN ltv_cte AS l
        ON f.singular_user_id = l.singular_user_id
)

SELECT 
    *
FROM final_cte;