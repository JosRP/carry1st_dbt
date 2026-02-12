create or replace view CARRY1ST_PLATFORM.REFINED.DISTRIBUTION_API__RELEVANT_LIST as

WITH used_cte AS (
    SELECT 
        DISTINCT 
        provider_id,
        prov_sku_id
    FROM carry1st_platform.refined.cogs__full
),

ranked AS (
    SELECT
        u.prov_date,                  
        u.c1st_prov_id,                              
        u.prov_sku_id,                   
        u.provider_name,                        
        u.prov_prod_name,                       
        u.prov_prod_name_2,                     
        u.face_value,                    
        u.face_value_cy,                  
        u.srp,
        u.srp_cy,
        u.cogs,
        u.cogs_cy,                        
        u.cogs_usd,
        u.c1st_margin,
        u.volume_type,
        CASE 
            WHEN c.provider_id IS NOT NULL THEN 1
            ELSE 0
            END AS shop_used_flag,
        CASE 
            WHEN u.product_type IN ('Top Up', 'Recharges') THEN 'Top Up'
            WHEN u.product_type IN ('Gift Card') THEN 'Gift Card'
            WHEN u.product_type IN ('Unknown') THEN 'Gift Card'
            ELSE 'Gift Card'
            END AS product_type,
        ROW_NUMBER() OVER (
            PARTITION BY u.c1st_prov_id, u.prov_sku_id
            ORDER BY u.prov_date DESC
            ) AS rn,
    FROM CARRY1ST_PLATFORM.REFINED.DISTRIBUTION_API__RELEVANT_LIST_FULLTIME AS u
    LEFT JOIN used_cte AS c
        ON u.c1st_prov_id = c.provider_id::integer 
        AND u.prov_sku_id = c.prov_sku_id::integer
)

SELECT *
FROM ranked
WHERE rn = 1;