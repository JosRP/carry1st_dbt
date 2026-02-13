INSERT INTO carry1st_platform.refined.CRM__CAMPAIGNS 

-- SELECT
--     customer_id,
--     last_recipient,
--     push_token,
--     variant_group,
--     campaign_name,
--     send_date
-- FROM  carry1st_platform.refined.CRM__CAMPAIGNS

WITH trx_cte AS (
    SELECT  
        user_id,
        max_by(recipient_id, trx_datetime) AS last_recipient
    FROM carry1st_platform.refined.transaction_detail_s
    WHERE 1=1
        AND provider_name = 'Activision'
        AND reporting_flag = 'Yes'
        AND trx_date >= current_date()-31 -- Made purchase in the last X days
    GROUP BY 1
),

final_cte AS (
    SELECT
      t.user_id AS customer_id,
      t.last_recipient,
      MAX_BY(push_token, d.created_date) As push_token
    FROM trx_cte AS t
    LEFT JOIN carry1st_platform.raw.device As d
        ON t.user_id = d.customer_id
    LEFT JOIN carry1st_platform.raw.customer As c
        ON t.user_id = c.id
    WHERE 1=1
        AND c.country_code IN ('NG') -- Country
        AND c.status = 'ACTIVE' -- Active, let it be
        AND c.language_code = 'en' -- Language
        and t.user_id NOT IN (    
            SELECT 
                distinct customer_id
            FROM carry1st_platform.refined.CRM__CAMPAIGNS 
            where send_date >= current_date()-31 ) -- Not in a campaign in the last 31 days
    GROUP BY 1,2
    HAVING MAX_BY(push_token, d.created_date) IS NOT NULL
    LIMIT 8000
)

SELECT 
    customer_id,
    last_recipient,
    push_token,
    IFF(UNIFORM(0, 1, RANDOM()) = 0, 'Control', 'Test') AS variant_group,
    'lalalaala' AS campaign_name,
FROM final_cte
