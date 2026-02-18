CREATE OR REPLACE VIEW carry1st_platform.refined.crm_260218_ramadan_EG_new_account AS

WITH trx_cte AS (
    SELECT 
        DISTINCT 
        user_id
    FROM carry1st_platform.refined.transaction_detail_s
    WHERE 1=1
        AND reporting_flag = 'Yes'
        AND tech_gate_source = 'SHOP'
        AND user_id IS NOT NULL
),

final_cte AS (
    SELECT 
        c.id AS customer_id,
        c.country_code,
        c.language_code,
        MAX_BY(push_token, d.created_date) As push_token,
    FROM carry1st_platform.raw.customer As c
    LEFT JOIN carry1st_platform.raw.device As d
        ON c.id = d.customer_id
    WHERE 1=1
       AND c.country_code IN ('EG', 'SA')
        AND c.status = 'ACTIVE'
        AND c.language_code = 'ar'
        AND c.id NOT IN (    -- Exclude users who were sent campaigns in the last 8 days
            SELECT 
                DISTINCT 
                customer_id
            FROM carry1st_platform.refined.CRM__CAMPAIGNS 
            WHERE send_date >= date(sysdate())-8) 
        AND c.id NOT IN (SELECT user_id FROM trx_cte) -- Exclude users who made transactions
        AND c.created_date >= date(sysdate())-91 -- Include user who created account at least 8 days ago
    GROUP BY 1,2,3
    HAVING MAX_BY(push_token, d.created_date) IS NOT NULL
),

test_cte AS (
    SELECT 
        customer_id,
        null as last_recipient,
        push_token,
        CASE 
            WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) <= 0.8 THEN 'Test' 
            ELSE 'Control'
            END AS variant_group,
        '260218_ramadan_EG_new_account' AS campaign_name,
        DATE('2026-02-18') AS send_date,
        language_code,
        country_code,
        sysdate() AS creation_sys_datetime
    FROM final_cte
)

SELECT 

*
FROM test_cte
