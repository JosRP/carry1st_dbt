create or replace view CARRY1ST_PLATFORM.REFINED.TERA_MKTG_COSTS as

WITH date_range AS (
	SELECT 
	DATEADD(DAY, SEQ4(), '2022-01-01') AS calendar_date
	FROM TABLE(GENERATOR(ROWCOUNT => 10000))
),

calendar_cte AS (
    SELECT 
    	DATE(calendar_date) AS calendar_date
    FROM date_range
    WHERE calendar_date BETWEEN '2023-01-01' AND CURRENT_DATE() - 1
),

calendar_cte_1 AS (
	SELECT 
		calendar_date,
        TO_CHAR(DATE(calendar_date), 'YYYY-MM') AS calendar_year_month
	FROM calendar_cte
),

vip_costs_cte AS (
   SELECT
        DATE(TO_TIMESTAMP(REPLACE(created_date, 'Z ', ''), 'YYYY-MM-DD HH24:MI:SS.FF')) AS created_date,
        SUM(amount)/1000 AS vip_costs
    FROM CARRY1ST_PLATFORM.RAW.GEM_TRANSACTION AS g
    LEFT JOIN carry1st_platform.refined.gems_exceptions_upload AS e
        ON g.reference = e.reference 
    WHERE 1=1
        AND DATE(TO_TIMESTAMP(REPLACE(created_date, 'Z ', ''), 'YYYY-MM-DD HH24:MI:SS.FF'))  BETWEEN DATE('2023-01-01') AND DATEADD(DAY, -1, CURRENT_DATE())
        AND status = 'REDEEMED'
        AND reversed = false
        AND source IN ('ADMIN_ALLOCATION','VIP_REWARD')
        AND e.reference IS NULL
		AND (YEAR(DATE(TO_TIMESTAMP(REPLACE(created_date, 'Z ', ''), 'YYYY-MM-DD HH24:MI:SS.FF')) ) <= 2024 OR allocation_type NOT IN ('RESELLER_OFFLINE_PAYMENT','REFUND'))
        AND NOT (allocation_type = 'MARKETING' AND NOTE = 'Glovo')
    GROUP BY 1
),

campaign_costs_cte AS (
	SELECT 
		c.calendar_date,
		c.calendar_year_month,
		DAY(LAST_DAY(DATE(CONCAT("year_month",'-01')))) AS month_days,
		CAST("mktg_retail" AS INT) AS campaign_costs
	FROM calendar_cte_1 AS c
	LEFT JOIN GOOGLE_SHEET.RAW.CM3_MARKETING_SPEND AS m
		ON c.calendar_year_month = m."year_month"
),

campaign_costs_cte_1 AS ( 
	SELECT
		date(calendar_date) AS calendar_date,
		campaign_costs / month_days AS campaign_costs
	FROM campaign_costs_cte
),

ua_costs_cte_1 AS (
    SELECT 
		campaign_date AS ua_date,
		device AS device,
		CASE 
			WHEN device = 'WEB' THEN campaign_name
			WHEN device = 'APP' THEN creative_name
			END AS campaign_code,
        SUM(cost) AS ua_costs
    FROM CARRY1ST_PLATFORM.REFINED.SINGULAR_CAMPAIGN_CLEAN_S   
    WHERE 1=1
        AND DATE(campaign_date) BETWEEN DATE('2023-01-01') AND DATEADD(DAY, -1, CURRENT_DATE())
        AND campaign_id IS NOT NULL
    GROUP BY 1,2,3
),

ua_costs_cte_2 AS (
    SELECT 
		ua_date,
		CASE 
			WHEN LOWER(campaign_code) LIKE 'so_' OR t.type = 'Shop' THEN 'SHOP'  
			WHEN LOWER(campaign_code) LIKE 're_' OR t.type = 'Retail' THEN 'SHOP - Retail'  
			WHEN LOWER(campaign_code) LIKE 'p1_' OR t.type = 'Pay1st' THEN 'SHOP - Pay1st'
			ELSE 'SHOP'
			END AS business_unit, 
        SUM(ua_costs) AS ua_costs
    FROM ua_costs_cte_1 As u
	LEFT JOIN CARRY1ST_PLATFORM.REFINED.TERA_UA_MAPPING_UPLOAD AS t
		ON u.campaign_code = t.camp_key
		AND u.device = t.device
    WHERE 1=1
    GROUP BY 1,2
),

cdp_costs_cte AS (
    SELECT 
        created_date,
        SUM(amount_usd) As amount_usd
    FROM CARRY1ST_PLATFORM.REFINED.CDP_TRANSACTION_DETAIL
    WHERE 1=1
        AND source_name = 'Admin. All.'
        AND direction = 1
		AND (YEAR(created_date) <= 2024 OR allocation_type NOT IN ('REFUND'))
    GROUP BY 1
),

shop_split_cte AS (
	SELECT 
		trx_date,
		COALESCE(SUM(CASE 
				WHEN business_unit = 'Pay1st' THEN gmv_n_gems
				ELSE NULL
				END) / SUM(gmv_n_gems),0) AS shop_pay1st_weight,
		FROM CARRY1ST_PLATFORM.REFINED.TRANSACTION_DETAIL_S
		WHERE 1=1
			AND trx_date >= DATE('2023-01-01')
			AND reporting_flag = 'Yes'
			AND gate_source = 'SHOP'
		GROUP BY 1
),

union_cte AS (

	-- CAMPAIGN PAY1ST
	SELECT 
		'SHOP - Pay1st' AS business_unit,
		calendar_date AS date,

		campaign_costs * shop_pay1st_weight AS campaign_costs,
		0 AS vip_costs,
		0 AS ua_costs,
        0 AS cdp_costs
	FROM campaign_costs_cte_1 AS c
	LEFT JOIN shop_split_cte AS s
		ON c.calendar_date = s.trx_date

	UNION ALL 
	-- CAMPAIGN RETAIL
	SELECT 
		'SHOP - Retail' AS business_unit,
		calendar_date AS date,

		campaign_costs * (1 - shop_pay1st_weight) AS campaign_costs,
		0 AS vip_costs,
		0 AS ua_costs,
        0 AS cdp_costs
	FROM campaign_costs_cte_1 AS c
	LEFT JOIN shop_split_cte AS s
        ON c.calendar_date = s.trx_date 

	UNION ALL 
	-- VIP PAY1ST
    SELECT		
		'SHOP - Pay1st' AS business_unit,
        created_date As date,

		0 AS campaign_costs,
		vip_costs * shop_pay1st_weight AS vip_costs,
		0 AS ua_costs,
        0 AS cdp_costs
	FROM vip_costs_cte AS v
	LEFT JOIN shop_split_cte AS s
		ON v.created_date = s.trx_date
    WHERE 1=1

	UNION ALL
	-- VIP RETAIL
	SELECT		
		'SHOP - Retail' AS business_unit,
        created_date As date,

		0 AS campaign_costs,
		vip_costs * (1 - shop_pay1st_weight) AS vip_costs,
		0 AS ua_costs,
        0 AS cdp_costs
	FROM vip_costs_cte AS v
	LEFT JOIN shop_split_cte AS s
		ON v.created_date = s.trx_date
    WHERE 1=1

	UNION ALL
	-- UA ALREADY SPLIT
	SELECT 
		business_unit, 
		ua_date AS date,

		0 AS campaign_costs,
		0 AS vip_costs,
		ua_costs AS ua_costs,
        0 AS cdp_costs
    FROM ua_costs_cte_2 As u
    WHERE 1=1
		and business_unit <> 'SHOP'

	UNION ALL 
	-- UA PAY1ST
	SELECT 
		'SHOP - Pay1st' business_unit, 
		ua_date AS date,

		0 AS campaign_costs,
		0 AS vip_costs,
		ua_costs * shop_pay1st_weight AS ua_costs,
        0 AS cdp_costs
    FROM ua_costs_cte_2 As u
    LEFT JOIN shop_split_cte AS s
		ON u.ua_date = s.trx_date
    WHERE 1=1
		and business_unit = 'SHOP'

	UNION ALL
	-- UA RETAIL
	SELECT 
		'SHOP - Retail' business_unit, 
		ua_date AS date,

		0 AS campaign_costs,
		0 AS vip_costs,
		ua_costs * (1 - shop_pay1st_weight) AS ua_costs,
        0 AS cdp_costs
    FROM ua_costs_cte_2 As u
    LEFT JOIN shop_split_cte AS s
		ON u.ua_date = s.trx_date
    WHERE 1=1
		and business_unit = 'SHOP'

    UNION ALL
        	-- CDP PAY1ST
	SELECT 
		'SHOP - Pay1st' business_unit, 
		created_date AS date,

		0 AS campaign_costs,
		0 AS vip_costs,
        0 AS ua_costs,
		amount_usd * shop_pay1st_weight AS cdp_costs 
    FROM cdp_costs_cte As u
    LEFT JOIN shop_split_cte AS s
		ON u.created_date = s.trx_date
    WHERE 1=1

	UNION ALL
	-- CDP RETAIL
	SELECT 
		'SHOP - Retail' business_unit, 
		created_date AS date,

		0 AS campaign_costs,
		0 AS vip_costs,
        0 AS ua_costs,
		amount_usd * (1 - shop_pay1st_weight) AS cdp_costs 
    FROM cdp_costs_cte As u
    LEFT JOIN shop_split_cte AS s
		ON u.created_date = s.trx_date
    WHERE 1=1
),

final_cte AS (
    SELECT 
    	business_unit,
    	DATE(date) AS date,
    	SUM(campaign_costs) AS campaign_costs,
    	SUM(vip_costs) AS vip_costs,
    	SUM(ua_costs) AS ua_costs,
        SUM(cdp_costs) AS cdp_costs
    FROM union_cte 
    GROUP BY 1,2
)

SELECT 
*
FROM final_cte;