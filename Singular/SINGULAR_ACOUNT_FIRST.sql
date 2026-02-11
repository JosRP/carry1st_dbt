CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.SINGULAR_ACOUNT_FIRST AS


WITH base_cte AS (
    SELECT
        DISTINCT
        session_datetime,
        singular_sid,
        session_date,
        CONCAT_WS('||',final_source, final_campaign, final_sub_campaign, final_creative,singular_campaign_bool) AS detail_concat,
        shop_user_id,
        platform
    FROm CARRY1ST_PLATFORM.REFINED.SINGULAR_EVENT_DETAIL_S 
    WHERE 1=1
        AND shop_user_id IS NOT NULL
        AND REGEXP_LIKE(shop_user_id, '^[0-9]+$') = true
),

first_cte AS (
    SELECT 
        DISTINCT
        shop_user_id,
        FIRST_VALUE(detail_concat) IGNORE NULLS OVER(PARTITION BY shop_user_id ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS detail_concat,
        FIRST_VALUE(platform) IGNORE NULLS OVER(PARTITION BY shop_user_id ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS platform,          
        FIRST_VALUE(singular_sid) IGNORE NULLS OVER(PARTITION BY shop_user_id ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS singular_sid,          
        FIRST_VALUE(session_date) IGNORE NULLS OVER(PARTITION BY shop_user_id ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_date,
        FIRST_VALUE(session_datetime) IGNORE NULLS OVER(PARTITION BY shop_user_id ORDER BY session_datetime ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS session_datetime
            
    FROM base_cte
)

SELECT 
    shop_user_id,
    platform,
    REPLACE(SPLIT(detail_concat, '||')[0], '"','') AS detail_source,
    REPLACE(SPLIT(detail_concat, '||')[1], '"','') AS detail_campaign,
    REPLACE(SPLIT(detail_concat, '||')[2], '"','') AS detail_sub_campaign,
    REPLACE(SPLIT(detail_concat, '||')[3], '"','') AS detail_creative,
    REPLACE(SPLIT(detail_concat, '||')[4], '"','') AS singular_flag,    
    session_datetime,
    session_date,
    singular_sid
FROM first_cte;