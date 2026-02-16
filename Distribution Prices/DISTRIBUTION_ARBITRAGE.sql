CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.DISTRIBUTION_ARBITRAGE AS

WITH calculated_metrics AS (
    SELECT 
        d.provider_name,
        COALESCE(p.prod_map, d.prov_prod_name) AS prod_map,
        COALESCE(d.face_value_cy, p.FACE_VALUE_CY_MAP) AS face_value_cy,
        d.prov_prod_name,
        d.prov_prod_name_2,
        d.face_value,
        d.cogs_usd,
        d.c1st_prov_id,
        d.prov_sku_id,
        -- Yield: How much Currency Value do we get for $1 USD?
        (d.face_value / NULLIF(d.cogs_usd, 0)) AS val_per_usd_ratio
    FROM CARRY1ST_PLATFORM.REFINED.DISTRIBUTION_API__RELEVANT_LIST AS d
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.DIST_PRICES_PROD_MAP AS p
        ON d.c1st_prov_id = p.provider_id
        AND d.prov_sku_id = p.prov_sku_id
    WHERE d.provider_name IN ('GAMERSMARKET', 'WG Cards', 'SEAGM')
      AND d.cogs_usd > 0
),

ranked_products AS (
    SELECT 
        *,
        -- Rank 1 is the BEST price (Highest Ratio) for that Provider/Product/Currency
        ROW_NUMBER() OVER (
            PARTITION BY provider_name, prod_map, face_value_cy 
            ORDER BY val_per_usd_ratio DESC
        ) as rn
    FROM calculated_metrics
),

best_per_provider AS (
    -- Filter to keep only the single best SKU per provider
    SELECT * FROM ranked_products WHERE rn = 1
),

pivoted_view AS (
    SELECT 
        prod_map,
        face_value_cy,
        
        -- GAMERSMARKET Columns
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN val_per_usd_ratio END) AS GM_Yield,
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN face_value END) AS GM_FaceVal,
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN cogs_usd END) AS GM_Cost,
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN prov_prod_name END) AS GM_Name_1,
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN prov_prod_name_2 END) AS GM_Name_2,
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN c1st_prov_id END) AS GM_Prov_ID,
        MAX(CASE WHEN provider_name = 'GAMERSMARKET' THEN prov_sku_id END) AS GM_SKU_ID,

        -- WG Cards Columns
        MAX(CASE WHEN provider_name = 'WG Cards' THEN val_per_usd_ratio END) AS WG_Yield,
        MAX(CASE WHEN provider_name = 'WG Cards' THEN face_value END) AS WG_FaceVal,
        MAX(CASE WHEN provider_name = 'WG Cards' THEN cogs_usd END) AS WG_Cost,
        MAX(CASE WHEN provider_name = 'WG Cards' THEN prov_prod_name END) AS WG_Name_1,
        MAX(CASE WHEN provider_name = 'WG Cards' THEN prov_prod_name_2 END) AS WG_Name_2,
        MAX(CASE WHEN provider_name = 'WG Cards' THEN c1st_prov_id END) AS WG_Prov_ID,
        MAX(CASE WHEN provider_name = 'WG Cards' THEN prov_sku_id END) AS WG_SKU_ID,

        -- SEAGM Columns
        MAX(CASE WHEN provider_name = 'SEAGM' THEN val_per_usd_ratio END) AS SEAGM_Yield,
        MAX(CASE WHEN provider_name = 'SEAGM' THEN face_value END) AS SEAGM_FaceVal,
        MAX(CASE WHEN provider_name = 'SEAGM' THEN cogs_usd END) AS SEAGM_Cost,
        MAX(CASE WHEN provider_name = 'SEAGM' THEN prov_prod_name END) AS SEAGM_Name_1,
        MAX(CASE WHEN provider_name = 'SEAGM' THEN prov_prod_name_2 END) AS SEAGM_Name_2,
        MAX(CASE WHEN provider_name = 'SEAGM' THEN c1st_prov_id END) AS SEAGM_Prov_ID,
        MAX(CASE WHEN provider_name = 'SEAGM' THEN prov_sku_id END) AS SEAGM_SKU_ID,

        -- Aggregate Stats for Spread Calculation (Ignores NULLs automatically)
        MIN(val_per_usd_ratio) AS min_yield_available,
        MAX(val_per_usd_ratio) AS max_yield_available,
        COUNT(DISTINCT provider_name) AS provider_count

    FROM best_per_provider
    GROUP BY prod_map, face_value_cy
)

SELECT 
    GM_Prov_ID,
    GM_SKU_ID,
    WG_Prov_ID,
    WG_SKU_ID,
    SEAGM_Prov_ID,
    SEAGM_SKU_ID,

    prod_map,
    face_value_cy,
    

    ROUND(GM_Yield, 2) AS GM_Yield,
    GM_FaceVal,
    ROUND(GM_Cost, 2) AS GM_Cost,

    GM_Name_1,
    GM_Name_2,
    
    ROUND(WG_Yield, 2) AS WG_Yield,
    WG_FaceVal,
    ROUND(WG_Cost, 2) AS WG_Cost,

    WG_Name_1,
    WG_Name_2,
    
    ROUND(SEAGM_Yield, 2) AS SEAGM_Yield,
    SEAGM_FaceVal,
    ROUND(SEAGM_Cost, 2) AS SEAGM_Cost,

    SEAGM_Name_1,
    SEAGM_Name_2,

    ROUND(
        (max_yield_available - min_yield_available) 
        / NULLIF(min_yield_available, 0) * 100, 
    2) AS spread_percentage

FROM pivoted_view
WHERE 1=1
    AND provider_count > 1