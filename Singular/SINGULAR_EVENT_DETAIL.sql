CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.SINGULAR_EVENT_DETAIL AS

WITH event_base AS (
    SELECT 
        session_date,
        session_datetime,
        device,
        user,
        trx_id,
        singular_sid,
        shop_user_id,
    
        campaign_id,
        sub_campaign_id,
        creative_id,
    
        utm_medium,
        utm_source,
        utm_campaign,
    
        unattributed_flag,

        country,
        platform
    FROM CARRY1ST_PLATFORM.REFINED.SINGULAR_EVENT_CLEAN_S
),

campaign_base AS (
    SELECT 
        campaign_date,
        device, 
    
        campaign_id,
        sub_campaign_id,
        creative_id,
        
        singular_source,
        campaign_name,
        sub_campaign_name,
        creative_name,
        
        impressions, 
        clicks,
        cost 
    FROM CARRY1ST_PLATFORM.REFINED.SINGULAR_CAMPAIGN_CLEAN_S
),

unnatributed_campaign_split AS (
    SELECT 
        campaign_date,
        device,

        singular_source,
        
        campaign_id,
        sub_campaign_id,
        creative_id, 
    
        campaign_name,
        sub_campaign_name,
        creative_name,
    
        SUM(clicks) AS clicks
    FROM CARRY1ST_PLATFORM.REFINED.SINGULAR_CAMPAIGN_CLEAN_S
    WHERE 1=1
    GROUP BY 1,2,3,4,5,6,7,8,9
),

date_fix_event AS (
    SELECT 
        DISTINCT
        session_date
    FROM event_base
    WHERE 1=1
        AND campaign_id IS NULL
        AND unattributed_flag = 1
        AND device = 'APP'
),

date_fix_campaign AS (
    SELECT 
        DISTINCT 
        campaign_date
    FROM unnatributed_campaign_split
    WHERE 1=1
        AND device = 'APP'
        AND singular_source = 'Facebook'
),

date_fix_both AS (
    SELECT 
        session_date,
        campaign_date,
        CASE 
            WHEN campaign_date IS NULL AND session_date <= '2023-03-31' THEN '2023-04-01'
            WHEN campaign_date IS NULL THEN LAG(campaign_date) IGNORE NULLS OVER(ORDER BY session_date ASC)
            ELSE session_date
            END as date_fix
    FROM date_fix_event AS e
    FULL JOIN date_fix_campaign AS c
        ON e.session_date = c.campaign_date
),

campaign_weights_1 AS (
    SELECT  
       campaign_date,
       SUM(clicks) AS day_clicks
    FROM unnatributed_campaign_split
    WHERE 1=1
        AND device = 'APP'
        AND singular_source = 'Facebook'
    GROUP BY 1
),

campagin_weights_2 AS (
    SELECT
        u.campaign_date,
        campaign_id,
        sub_campaign_id,
        creative_id,
        SUM(clicks) As original_clics,
        SUM(day_clicks) AS day_clicks,
        SUM(clicks) / SUM(day_clicks) AS campaign_weight
    FROM unnatributed_campaign_split AS u
    LEFT JOIN campaign_weights_1 AS c
        ON u.campaign_date = c.campaign_date
    WHERE 1=1
        AND u.device = 'APP'
        AND u.singular_source = 'Facebook'
    GROUP BY 1,2,3,4
),

pre_date_fix_implement AS (
    SELECT 
        DISTINCT 
            singular_sid,
            session_date
    FROM event_base
    WHERE 1=1
        AND campaign_id IS NULL
        AND unattributed_flag = 1
        AND device = 'APP'
),   

date_fix_implement AS (
    SELECT 
        DISTINCT
        singular_sid,
        date_fix,
        ROW_NUMBER() OVER(PARTITION BY date_fix ORDER BY 
            ABS(TO_NUMBER(SUBSTRING(TO_VARCHAR(REPLACE(HASH(singular_sid, 'SHA1'),'-','')), 2, 10), 'XXXXXXXXXXXXXXXXX')) ASC) AS row_numb, -- This was changed
    FROM pre_date_fix_implement AS e
    LEFT JOIN date_fix_both AS d
        on e.session_date = d.session_date
    WHERE 1=1
),

norm_row_cte AS (
    SELECT 
        date_fix,
        MAX(row_numb) AS max_row_numb,
        MIN(row_numb) AS min_row_numb
    FROM date_fix_implement
    GROUP BY 1
),

prob_calc_cte AS (
    SELECT
        d.singular_sid,
        d.date_fix,
        COALESCE((row_numb - min_row_numb) / NULLIF((max_row_numb - min_row_numb), 0),0) AS prob
    FROM date_fix_implement AS d
    LEFT JOIN norm_row_cte AS n
        ON d.date_fix = n.date_fix
), 

random_cte_1 AS (
    SELECT
        d.singular_sid,
        campaign_id,
        sub_campaign_id,
        creative_id,
        prob,
        SUM(campaign_weight) OVER (PARTITION BY d.singular_sid ORDER BY campaign_id, sub_campaign_id, creative_id) AS cum_weight
    FROM date_fix_implement AS d
    LEFT JOIN campagin_weights_2 AS c
        ON d.date_fix = c.campaign_date
    LEFT JOIN prob_calc_cte AS p
        ON d.singular_sid = p.singular_sid
),

random_cte_2 AS (
    SELECT 
            singular_sid,
            campaign_id,
            sub_campaign_id,
            creative_id,
            prob,
            cum_weight,
            LAG(cum_weight, 1, 0) OVER (PARTITION BY singular_sid ORDER BY campaign_id,sub_campaign_id,creative_id) AS prev_cum_weight
    FROM random_cte_1
),

random_cte_3 AS (
    SELECT 
        singular_sid,
        campaign_id,
        sub_campaign_id,
        creative_id
    FROM random_cte_2
     WHERE 1=1
        AND prob <= cum_weight
        AND prob > prev_cum_weight
),

campaign_id_split AS (
    SELECT 
        campaign_id,
        MAX(singular_source) AS singular_source,
        max(campaign_name) AS campaign_name_fix
    FROM unnatributed_campaign_split
    GROUP BY 1
),

sub_campaign_id_split AS (
    SELECT 
        sub_campaign_id,
        max(sub_campaign_name) AS sub_campaign_name_fix
    FROM unnatributed_campaign_split
    GROUP BY 1
),

creative_id_split AS (
    SELECT 
        creative_id,
        max(creative_name) AS creative_name_fix
    FROM unnatributed_campaign_split
    GROUP BY 1
),


final_cte AS (
    SELECT
    	e.session_date,
    	e.session_datetime,
    	e.device,
    	e.user,
    	e.trx_id,
    	e.singular_sid,
    	e.shop_user_id,
        e.country,
        e.platform,

        
        REPLACE(COALESCE(r.campaign_id, e.campaign_id), '"','') AS campaign_id,
        REPLACE(COALESCE(r.sub_campaign_id, e.sub_campaign_id), '"','') AS sub_campaign_id,
        REPLACE(e.creative_id, '"','') AS creative_id,

        REPLACE(e.utm_medium, '"','')  AS utm_medium,
        REPLACE(e.utm_source, '"','')  AS utm_source,
        REPLACE(e.utm_campaign, '"','')  AS utm_campaign,

        REPLACE(f1.singular_source, '"','') AS singular_source,
        REPLACE(f1.campaign_name_fix, '"','')  AS campaign_name,
        REPLACE(f2.sub_campaign_name_fix, '"','')  AS sub_campaign_name,
        REPLACE(f3.creative_name_fix, '"','')  AS creative_name,
    
    	COALESCE(CASE 
    	WHEN f1.campaign_name_fix IS NOT NULL 
    		THEN 1
    	ELSE 0
    	END,0) AS singular_campaign_bool,

        COALESCE(REPLACE(f1.singular_source, '"',''),REPLACE(e.utm_source, '"',''),'Organic') AS final_source,
        COALESCE(REPLACE(f1.campaign_name_fix, '"',''),REPLACE(e.utm_campaign,'"',''),'Organic') AS final_campaign,
        COALESCE(REPLACE(f2.sub_campaign_name_fix, '"',''),REPLACE(e.utm_medium,'"',''),'Organic') AS final_sub_campaign,
        COALESCE(REPLACE(f3.creative_name_fix, '"',''),'None') AS final_creative,

        CASE 
            WHEN r.singular_sid  IS NOT NULL THEN 1
            ELSE 0 
            END AS random_flag
    FROM event_base AS e
    LEFT JOIN random_cte_3 AS r
        ON e.singular_sid = r.singular_sid    
    LEFT JOIN campaign_id_split AS f1
        ON COALESCE(r.campaign_id, e.campaign_id) = f1.campaign_id
    LEFT JOIN sub_campaign_id_split AS f2
        ON COALESCE(r.sub_campaign_id, e.sub_campaign_id) = f2.sub_campaign_id
    LEFT JOIN creative_id_split AS f3
        ON COALESCE(r.creative_id, e.creative_id) = f3.creative_id
)

SELECT 
* 
FROM final_cte;