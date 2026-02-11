CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.SINGULAR_TRX_DETAIL AS 

WITH base_cte AS (
    SELECT
        trx_id,
        MAX(CONCAT_WS('||',final_source, final_campaign, final_sub_campaign, final_creative,singular_campaign_bool)) AS detail_concat,
        MAX(country) As ip_country
    FROM CARRY1ST_PLATFORM.REFINED.SINGULAR_EVENT_DETAIL_S
    GROUP BY 1
) 

SELECT 
    trx_id,
    REPLACE(SPLIT(detail_concat, '||')[0], '"','') AS detail_source,
    REPLACE(SPLIT(detail_concat, '||')[1], '"','') AS detail_campaign,
    REPLACE(SPLIT(detail_concat, '||')[2], '"','') AS detail_sub_campaign,
    REPLACE(SPLIT(detail_concat, '||')[3], '"','') AS detail_creative,
    REPLACE(SPLIT(detail_concat, '||')[4], '"','') AS singular_flag, 
    ip_country
FROM base_cte;