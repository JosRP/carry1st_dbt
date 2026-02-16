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
        MAX_BY(push_token, d.created_date) As push_token,
    FROM carry1st_platform.raw.customer As c
    LEFT JOIN carry1st_platform.raw.device As d
        ON c.id = d.customer_id
    WHERE 1=1
        AND c.country_code IN ('EG')
        AND c.status = 'ACTIVE'
        AND c.id NOT IN (    -- Exclude users who were sent campaigns in the last 8 days
            SELECT 
                DISTINCT 
                customer_id
            FROM carry1st_platform.refined.CRM__CAMPAIGNS 
            WHERE send_date >= date(sysdate())-8) 
        AND c.id NOT IN (SELECT user_id FROM trx_cte) -- Exclude users who made transactions
        AND c.created_date >= date(sysdate())-8 -- Include user who created account at least 8 days ago
    GROUP BY 1
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
        '260218_ramadan_EG_new_account' AS campaign_name
    FROM final_cte
)

SELECT 
    *
FROM test_cte
