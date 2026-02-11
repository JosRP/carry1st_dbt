CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.SINGULAR_CAMPAIGN_CLEAN AS

WITH campaign_cte AS (
    SELECT
        DATE(date) AS campaign_date,
        CASE 
            WHEN app = 'Carry1st Shop - Web' THEN 'WEB' 
            WHEN app = 'Carry1st Shop - Mobile' THEN 'APP'
            END AS device, 
            
        source AS singular_source,

        adn_campaign_id AS campaign_id,
        adn_sub_campaign_id AS sub_campaign_id,
        adn_creative_id AS creative_id,
        
        adn_campaign_name AS campaign_name,
        adn_sub_campaign_name AS sub_campaign_name,
        adn_creative_name AS creative_name,

        country_field AS country,

        SUM(adn_impressions) AS impressions, 
        SUM(adn_clicks) AS clicks,
        SUM(adn_cost) AS cost
    FROM SINGULAR.SINGULAR.CUSTOM_CAMPAIGN_CREATIVE_V1
    WHERE 1=1   
        AND DATE(date) <= SYSDATE()::date -1 
        AND adn_campaign_id IS NOT NULL
        AND app IN ('Carry1st Shop - Web', 'Carry1st Shop - Mobile')
    GROUP BY 1,2,3,4,5,6,7,8,9,10
),

campaign_fix_cte AS (
    SELECT
        campaign_id,
        MAX_BY(singular_source, campaign_date) AS singular_source,
        MAX_BY(campaign_name, campaign_date) AS campaign_name_fix
    FROM campaign_cte
    GROUP BY 1
),

sub_campaign_fix_cte AS (
    SELECT
        sub_campaign_id,
        MAX_BY(sub_campaign_name, campaign_date) AS sub_campaign_name_fix
    FROM campaign_cte
    GROUP BY 1
),

creative_fix_cte AS (
    SELECT
        creative_id,
        MAX_BY(creative_name, campaign_date) AS creative_name_fix
    FROM campaign_cte
    GROUP BY 1
),

final_cte AS (
    SELECT 
        campaign_date,
        device, 
   
        b.campaign_id,
        b.sub_campaign_id,
        b.creative_id,

        g.ALPHA_2_CODE AS country,

        REPLACE(f1.singular_source, '"','') AS singular_source,
        REPLACE(f1.campaign_name_fix, '"','')  AS campaign_name,
        REPLACE(f2.sub_campaign_name_fix, '"','')  AS sub_campaign_name,
        REPLACE(f3.creative_name_fix, '"','')  AS creative_name,
        
        SUM(impressions) AS impressions, 
        SUM(clicks) AS clicks,
        SUM(cost) AS cost
    FROM campaign_cte AS b
    LEFT JOIN campaign_fix_cte AS f1
        ON b.campaign_id = f1.campaign_id
    LEFT JOIN sub_campaign_fix_cte AS f2
        ON b.sub_campaign_id = f2.sub_campaign_id
    LEFT JOIN creative_fix_cte AS f3
        ON b.creative_id = f3.creative_id
    LEFT JOIN  CARRY1ST_PLATFORM.REFINED.DIM_GEO As g
        ON b.country = g.APHA_3_CODE
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT 
*
FROM final_cte;