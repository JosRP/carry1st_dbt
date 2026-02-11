CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.SINGULAR_EVENT_CLEAN AS 

WITH base_cte AS (
	SELECT 
        DISTINCT
		DATE(adjusted_timestamp) AS session_date,
		adjusted_timestamp AS session_datetime,
        CASE 
            WHEN app_longname = 'https://shop.carry1st.com/' THEN 'WEB' 
            WHEN app_longname = 'com.carry1st.shop' THEN 'APP'
            END AS device,

		device_id AS user,
		CASE 
            WHEN name = 'purchase' THEN arguments:transaction_id::string
            ELSE NULL
            END AS trx_id,
    	IFF(session_id = 'null', NULL, session_id) AS session_id,
		custom_user_id AS shop_user_id,

		campaign_id,
		sub_campaign_id,
		creative_id,

		utm_source,
		utm_medium,
		utm_campaign,

        IFF(country='--', NULL, country) AS country,
        platform,

        CASE 
            WHEN lower(partner) = 'unattributed' THEN 1
            ELSE 0 
            END AS unattributed_flag 

	FROM SINGULAR.REFINED.EVENT
	WHERE 1=1
		AND app_longname IN ('https://shop.carry1st.com/', 'com.carry1st.shop')
 		AND date(adjusted_timestamp) <=SYSDATE()::date -1 
),

session_id_fix_cte AS (
	SELECT
		session_date,
		session_datetime,
		device,
		user,
		trx_id,
		session_id,
		campaign_id,
		sub_campaign_id,
		creative_id,
		shop_user_id,

		utm_source,
		utm_medium,
		utm_campaign,

        unattributed_flag,

        country,
        platform,

		CASE 
            WHEN session_id IS NOT NULL THEN session_id
            ELSE LAG(session_id ) IGNORE NULLS OVER (PARTITION BY user ORDER BY session_datetime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
            END AS session_id_fix
	FROM base_cte
),

base_cte_2 AS (
	SELECT
		session_date,
        session_datetime,
		device,
		user,
		trx_id,
		session_id,

		campaign_id,
		sub_campaign_id,
		creative_id,

		utm_source,
		utm_medium,
		utm_campaign,

        country,
        platform,

		shop_user_id,
		CONCAT_WS('_', user, COALESCE(session_id_fix,CONCAT_WS('_','missing',session_date))) AS singular_sid,
        unattributed_flag,
	FROM session_id_fix_cte
),

campaign_id_fix_cte_2 As (
    SELECT
        singular_sid,
		MIN(session_date) AS session_date_fix,
        MAX(CONCAT_WS('||', campaign_id, COALESCE(sub_campaign_id,''), COALESCE(creative_id,''))) AS campaign_concat,
		MAX(CONCAT_WS('||', COALESCE(utm_medium,''), COALESCE(utm_source,''), COALESCE(utm_campaign,''))) AS utm_concat,
        MAX(unattributed_flag) AS unattributed_flag,
        MAX(country) AS country,
        MAX(platform) AS platform
    FROM base_cte_2
    GROUP BY 1
),

event_final_cte AS (
    SELECT
        session_date_fix AS session_date,
        session_datetime,
        device,
        user,
        trx_id,
        b.singular_sid,
		shop_user_id,

        SPLIT(f.campaign_concat, '||')[0] AS campaign_id,
        CASE WHEN SPLIT(f.campaign_concat, '||')[1] = '' THEN NULL ELSE SPLIT(f.campaign_concat, '||')[1] END AS sub_campaign_id,
        CASE WHEN SPLIT(f.campaign_concat, '||')[2] = '' THEN NULL ELSE SPLIT(f.campaign_concat, '||')[2] END AS creative_id,

		SPLIT(f.utm_concat, '||')[0] AS utm_medium,
        CASE WHEN SPLIT(f.utm_concat, '||')[1] = '' THEN NULL ELSE SPLIT(f.utm_concat, '||')[1] END AS utm_source,
        CASE WHEN SPLIT(f.utm_concat, '||')[2] = '' THEN NULL ELSE SPLIT(f.utm_concat, '||')[2] END AS utm_campaign,

		f.unattributed_flag,
        f.country,
        f.platform
    FROM base_cte_2 AS b
    LEFT JOIN campaign_id_fix_cte_2 AS f
    	ON b.singular_sid = f.singular_sid
)

SELECT 
	* 
FROM event_final_cte