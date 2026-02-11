create or replace VIEW carry1st_platform.refined.T_CAC_REPORT AS

WITH url_cte As (
    SELECT 
        trx_id,
        detail_source,
        detail_campaign,
        detail_sub_campaign,
        detail_creative,
        ip_country,
        IFF(singular_flag = 1, 'Yes', 'No') AS singular_flag,
    FROM carry1st_platform.refined.singular_trx_detail_s
    WHERE 1=1
),

trx_cte AS (
    SELECT 
        trx_date,
        trx_id,
        reference,
        trx_datetime,
        email,
        SUM(COALESCE(gmv, 0)) AS gmv,
        SUM(COALESCE(revenue, 0)) AS revenue,
        SUM(COALESCE(gp, 0)) AS gp,
    FROM carry1st_platform.refined.transaction_detail_s AS t
    WHERE 1=1
        AND gems_flag = 'No'
        AND reporting_flag = 'Yes'
        AND tech_gate_source = 'SHOP'
    GROUP BY 1,2,3,4,5
),

trx_bonus_cte AS (
    SELECT 
        t.trx_date,
        t.trx_id,
        t.reference,
        t.trx_datetime,
        t.email,
        t.gmv AS gmv,
        t.revenue,
        t.gp, 
        t.gp + COALESCE(b.trx_codm_bonus,0) AS gp_bonus,
    FROM trx_cte AS t
    LEFT JOIN carry1st_platform.refined.pnl__codm_bonus_trx AS b
        ON t.reference = b.reference
),

first_cte AS (
    SELECT
        email,
        MIN(trx_date) AS first_trx_date,
        MIN_BY(trx_id, trx_datetime) as first_trx_id
    FROM trx_bonus_cte
    GROUP BY 1
),

campaign_cte AS (
    SELECT 
        campaign_date,
        device,
        SINGULAR_SOURCE,
        campaign_NAME,
        sub_campaign_name,
        creative_name,
        country,
        SUM(COALESCE(IMPRESSIONS,0)) As t_impressions,
        SUM(COALESCE(CLICKS,0)) AS t__clicks,
        SUM(COALESCE(COST,0)) AS t_cost,
    FROM carry1st_platform.refined.singular_campaign_clean_s 
    GROUP BY 1,2,3,4,5,6,7
),

creative_dates_cte AS (
    SELECT 
        singular_source,
        campaign_NAME,
        sub_campaign_name,
        creative_name,
        MIN(campaign_date) AS creative_min_date,
        MAX(campaign_date) AS creative_max_date,
    FROM campaign_cte
    GROUP BY 1,2,3,4
),

campaign_dates_cte AS (
    SELECT 
        singular_source,
        campaign_NAME,
        MIN(campaign_date) AS campaign_min_date,
        MAX(campaign_date) AS campaign_max_date,
    FROM campaign_cte
    GROUP BY 1,2
),

device_cte AS (
    SELECT 
        campaign_NAME,
        MAX(device) AS device
    FROM carry1st_platform.refined.singular_campaign_clean_s 
    GROUP BY 1
),

first_join AS (
    SELECT 
        f.first_trx_date,
        COALESCE(u.detail_source, 'Not Tracked') AS detail_source,
        COALESCE(u.detail_campaign, 'Not Tracked') AS detail_campaign,
        COALESCE(u.detail_sub_campaign, 'Not Tracked') AS detail_sub_campaign,
        COALESCE(u.detail_creative, 'Not Tracked') AS detail_creative,
        COALESCE(singular_flag, 'No') AS singular_flag,
        COALESCE(d.device, 'Unknown') AS device,
        COALESCE(u.ip_country, 'Unknown') AS ip_country,

        COUNT(DISTINCT t.reference) AS t_trxs,
        COUNT(DISTINCT t.email) AS t_customers,
        SUM(COALESCE(t.gmv,0)) AS t_gmv,
        SUM(COALESCE(t.revenue,0)) AS t_revenue,
        SUM(COALESCE(t.gp,0)) AS t_gp,
        SUM(COALESCE(t.gp_bonus,0)) AS t_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 1, t.gmv, 0)) AS d1_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 1, t.revenue, 0)) AS d1_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 1, t.gp, 0)) AS d1_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 1, t.gp_bonus, 0)) AS d1_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 7, t.gmv, 0)) AS d7_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 7, t.revenue, 0)) AS d7_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 7, t.gp, 0)) AS d7_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 7, t.gp_bonus, 0)) AS d7_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 14, t.gmv, 0)) AS d14_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 14, t.revenue, 0)) AS d14_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 14, t.gp, 0)) AS d14_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 14, t.gp_bonus, 0)) AS d14_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 30, t.gmv, 0)) AS d30_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 30, t.revenue, 0)) AS d30_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 30, t.gp, 0)) AS d30_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 30, t.gp_bonus, 0)) AS d30_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 60, t.gmv, 0)) AS d60_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 60, t.revenue, 0)) AS d60_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 60, t.gp, 0)) AS d60_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 60, t.gp_bonus, 0)) AS d60_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 90, t.gmv, 0)) AS d90_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 90, t.revenue, 0)) AS d90_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 90, t.gp, 0)) AS d90_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 90, t.gp_bonus, 0)) AS d90_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 120, t.gmv, 0)) AS d120_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 120, t.revenue, 0)) AS d120_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 120, t.gp, 0)) AS d120_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 120, t.gp_bonus, 0)) AS d120_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 150, t.gmv, 0)) AS d150_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 150, t.revenue, 0)) AS d150_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 150, t.gp, 0)) AS d150_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 150, t.gp_bonus, 0)) AS d150_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 180, t.gmv, 0)) AS d180_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 180, t.revenue, 0)) AS d180_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 180, t.gp, 0)) AS d180_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 180, t.gp_bonus, 0)) AS d180_gp_bonus,

        -- NEW D210, D240, D270 CALCULATIONS
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 210, t.gmv, 0)) AS d210_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 210, t.revenue, 0)) AS d210_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 210, t.gp, 0)) AS d210_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 210, t.gp_bonus, 0)) AS d210_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 240, t.gmv, 0)) AS d240_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 240, t.revenue, 0)) AS d240_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 240, t.gp, 0)) AS d240_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 240, t.gp_bonus, 0)) AS d240_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 270, t.gmv, 0)) AS d270_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 270, t.revenue, 0)) AS d270_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 270, t.gp, 0)) AS d270_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 270, t.gp_bonus, 0)) AS d270_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 360, t.gmv, 0)) AS d360_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 360, t.revenue, 0)) AS d360_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 360, t.gp, 0)) AS d360_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 360, t.gp_bonus, 0)) AS d360_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 450, t.gmv, 0)) AS d450_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 450, t.revenue, 0)) AS d450_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 450, t.gp, 0)) AS d450_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 450, t.gp_bonus, 0)) AS d450_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 540, t.gmv, 0)) AS d540_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 540, t.revenue, 0)) AS d540_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 540, t.gp, 0)) AS d540_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 540, t.gp_bonus, 0)) AS d540_gp_bonus,

        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 720, t.gmv, 0)) AS d720_gmv,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 720, t.revenue, 0)) AS d720_revenue,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 720, t.gp, 0)) AS d720_gp,
        SUM(IFF(DATEDIFF(day, first_trx_date, t.trx_date) < 720, t.gp_bonus, 0)) AS d720_gp_bonus

    FROM trx_bonus_cte AS t
    LEFT JOIN first_cte AS f
        ON t.email = f.email
    LEFT JOIN url_cte AS u
        ON f.first_trx_id = u.trx_id
    LEFT JOIN device_cte AS d
        ON u.detail_campaign = d.campaign_NAME
    GROUP BY 1,2,3,4,5,6,7,8
),

union_cte AS (
    SELECT 
        first_trx_date AS event_date,
        device AS device,
        detail_source AS singular_source,
        detail_campaign AS singular_campaign,
        detail_sub_campaign AS singular_sub_campaign,
        detail_creative AS singular_creative,
        singular_flag AS singular_flag,
        ip_country AS ip_country,

        0 AS t_impressions,
        0 AS t_clicks,
        0 AS t_cost,

        t_trxs,
        t_customers,
        t_gmv,
        t_revenue,
        t_gp,
        t_gp_bonus,
        d1_gmv, d1_revenue, d1_gp, d1_gp_bonus,
        d7_gmv, d7_revenue, d7_gp, d7_gp_bonus,
        d14_gmv, d14_revenue, d14_gp, d14_gp_bonus,
        d30_gmv, d30_revenue, d30_gp, d30_gp_bonus,
        d60_gmv, d60_revenue, d60_gp, d60_gp_bonus,
        d90_gmv, d90_revenue, d90_gp, d90_gp_bonus,
        d120_gmv, d120_revenue, d120_gp, d120_gp_bonus,
        d150_gmv, d150_revenue, d150_gp, d150_gp_bonus,
        d180_gmv, d180_revenue, d180_gp, d180_gp_bonus,
        -- Added to union
        d210_gmv, d210_revenue, d210_gp, d210_gp_bonus,
        d240_gmv, d240_revenue, d240_gp, d240_gp_bonus,
        d270_gmv, d270_revenue, d270_gp, d270_gp_bonus,
        d360_gmv, d360_revenue, d360_gp, d360_gp_bonus,
        d450_gmv, d450_revenue, d450_gp, d450_gp_bonus,
        d540_gmv, d540_revenue, d540_gp, d540_gp_bonus,
        d720_gmv, d720_revenue, d720_gp, d720_gp_bonus
    FROM first_join

    UNION ALL

    SELECT 
        campaign_date AS event_date,
        device AS device,
        SINGULAR_SOURCE AS singular_source,
        campaign_NAME AS singular_campaign,
        sub_campaign_NAME AS singular_sub_campaign,
        creative_name AS singular_creative,
        'Yes' AS singular_flag,
        country AS ip_country,

        t_impressions AS t_impressions,
        t__clicks AS t_clicks,
        t_cost AS t_cost,

        0 AS t_trxs,
        0 AS t_customers,
        0 AS t_gmv,
        0 AS t_revenue,
        0 AS t_gp,
        0 AS t_gp_bonus,
        0,0,0,0, -- d1
        0,0,0,0, -- d7
        0,0,0,0, -- d14
        0,0,0,0, -- d30
        0,0,0,0, -- d60
        0,0,0,0, -- d90
        0,0,0,0, -- d120
        0,0,0,0, -- d150
        0,0,0,0, -- d180
        0,0,0,0, -- d210
        0,0,0,0, -- d240
        0,0,0,0, -- d270
        0,0,0,0, -- d360
        0,0,0,0, -- d450
        0,0,0,0, -- d540
        0,0,0,0  -- d720
    FROM campaign_cte
)

SELECT 
    u.event_date,
    u.device,
    u.singular_source,
    u.singular_campaign,
    u.singular_sub_campaign,
    u.singular_creative,
    u.singular_flag,

    c.game_name,
    u.ip_country AS ip_country,
    g.country_slim AS ip_country_slim,

    d1.creative_min_date,
    d1.creative_max_date,
    d2.campaign_min_date,
    d2.campaign_max_date,

    IFF(d1.creative_max_date < current_date()-1, 'Yes', 'No') AS creative_active,
    IFF(d2.campaign_max_date < current_date()-1, 'Yes', 'No') AS campaign_active,

    SUM(u.t_impressions) AS t_impressions,
    SUM(u.t_clicks) AS t_clicks,
    SUM(u.t_cost) AS t_cost,
    SUM(u.t_trxs) AS t_trxs,
    SUM(u.t_customers) AS t_customers,
    SUM(u.t_gmv) AS t_gmv,
    SUM(u.t_revenue) AS t_revenue,
    SUM(u.t_gp) AS t_gp,
    SUM(u.t_gp_bonus) AS t_gp_bonus,

    SUM(u.d1_gmv) AS d1_gmv, SUM(u.d1_revenue) AS d1_revenue, SUM(u.d1_gp) AS d1_gp, SUM(u.d1_gp_bonus) AS d1_gp_bonus,
    SUM(u.d7_gmv) AS d7_gmv, SUM(u.d7_revenue) AS d7_revenue, SUM(u.d7_gp) AS d7_gp, SUM(u.d7_gp_bonus) AS d7_gp_bonus,
    SUM(u.d14_gmv) AS d14_gmv, SUM(u.d14_revenue) AS d14_revenue, SUM(u.d14_gp) AS d14_gp, SUM(u.d14_gp_bonus) AS d14_gp_bonus,
    SUM(u.d30_gmv) AS d30_gmv, SUM(u.d30_revenue) AS d30_revenue, SUM(u.d30_gp) AS d30_gp, SUM(u.d30_gp_bonus) AS d30_gp_bonus,
    SUM(u.d60_gmv) AS d60_gmv, SUM(u.d60_revenue) AS d60_revenue, SUM(u.d60_gp) AS d60_gp, SUM(u.d60_gp_bonus) AS d60_gp_bonus,
    SUM(u.d90_gmv) AS d90_gmv, SUM(u.d90_revenue) AS d90_revenue, SUM(u.d90_gp) AS d90_gp, SUM(u.d90_gp_bonus) AS d90_gp_bonus,
    SUM(u.d120_gmv) AS d120_gmv, SUM(u.d120_revenue) AS d120_revenue, SUM(u.d120_gp) AS d120_gp, SUM(u.d120_gp_bonus) AS d120_gp_bonus,
    SUM(u.d150_gmv) AS d150_gmv, SUM(u.d150_revenue) AS d150_revenue, SUM(u.d150_gp) AS d150_gp, SUM(u.d150_gp_bonus) AS d150_gp_bonus,
    SUM(u.d180_gmv) AS d180_gmv, SUM(u.d180_revenue) AS d180_revenue, SUM(u.d180_gp) AS d180_gp, SUM(u.d180_gp_bonus) AS d180_gp_bonus,
    
    -- FINAL AGGREGATION FOR 210, 240, 270
    SUM(u.d210_gmv) AS d210_gmv, SUM(u.d210_revenue) AS d210_revenue, SUM(u.d210_gp) AS d210_gp, SUM(u.d210_gp_bonus) AS d210_gp_bonus,
    SUM(u.d240_gmv) AS d240_gmv, SUM(u.d240_revenue) AS d240_revenue, SUM(u.d240_gp) AS d240_gp, SUM(u.d240_gp_bonus) AS d240_gp_bonus,
    SUM(u.d270_gmv) AS d270_gmv, SUM(u.d270_revenue) AS d270_revenue, SUM(u.d270_gp) AS d270_gp, SUM(u.d270_gp_bonus) AS d270_gp_bonus,
    
    SUM(u.d360_gmv) AS d360_gmv, SUM(u.d360_revenue) AS d360_revenue, SUM(u.d360_gp) AS d360_gp, SUM(u.d360_gp_bonus) AS d360_gp_bonus,
    SUM(u.d450_gmv) AS d450_gmv, SUM(u.d450_revenue) AS d450_revenue, SUM(u.d450_gp) AS d450_gp, SUM(u.d450_gp_bonus) AS d450_gp_bonus,
    SUM(u.d540_gmv) AS d540_gmv, SUM(u.d540_revenue) AS d540_revenue, SUM(u.d540_gp) AS d540_gp, SUM(u.d540_gp_bonus) AS d540_gp_bonus,
    SUM(u.d720_gmv) AS d720_gmv, SUM(u.d720_revenue) AS d720_revenue, SUM(u.d720_gp) AS d720_gp, SUM(u.d720_gp_bonus) AS d720_gp_bonus

FROM union_cte AS u
LEFT JOIN CARRY1ST_PLATFORM.REFINED.SINGULAR_creative_game AS c
    ON u.singular_campaign = c.campaign_NAME
    AND u.singular_creative = c.CREATIVE_NAME
LEFT JOIN carry1st_platform.refined.dim_geo AS g
    ON u.ip_country = g.ALPHA_2_CODE
LEFT JOIN creative_dates_cte AS d1
    ON u.singular_source = d1.singular_source
    AND u.singular_campaign = d1.campaign_NAME
    AND u.singular_sub_campaign = d1.sub_campaign_NAME
    AND u.singular_creative = d1.creative_name
LEFT JOIN campaign_dates_cte AS d2
    ON u.singular_source = d2.singular_source
    AND u.singular_campaign = d2.campaign_NAME
WHERE 1=1
    AND event_date BETWEEN '2024-01-01' AND CURRENT_DATE()-1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;