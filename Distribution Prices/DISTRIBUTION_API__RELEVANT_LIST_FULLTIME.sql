create or replace view CARRY1ST_PLATFORM.REFINED.DISTRIBUTION_API__RELEVANT_LIST_FULLTIME as 

WITH reward_base AS (
    SELECT 
        "load_date" AS prov_date,
        "name" As name,
        "id" AS sku_id,
        f.value:min::FLOAT AS min_price,
        f.value:max::FLOAT AS max_price,
        f.value:discount::FLOAT As disc,
        "currency" AS currency,
        CASE 
            WHEN "category" = 'Gift Card' THEN 'Gift Card'
            WHEN "category" = 'Recharges' THEN 'Top-Up' 
            ELSE 'Unknown'
            END As category
    FROM CARRY1ST_PLATFORM.RAW.REWARD_STORE_PRICES s,
        LATERAL FLATTEN(input => s."denominations", OUTER => TRUE) f
    WHERE 1=1
),

bamboo_cte AS (
    SELECT
        "load_date" AS prov_date,
        "name" AS global_name,
        "countryCode" AS country_code,
        "currencyCode" AS item_currency,
        f.value:id::int AS id,
        f.value:name::string AS sku_name,
        f.value:maxFaceValue::float AS max_face_value,
        f.value:minFaceValue::float AS min_face_value,
        f.value:price.currencyCode::string AS currency_code,
        f.value:price.max::float AS price_max,
        f.value:price.min::float AS price_min,
    FROM carry1st_platform.raw.bamboo_card_prices,
    LATERAL FLATTEN(input => "products") AS f
),

bamboo_fx_cte AS (
    SELECT  
        "currencyCode" As currency_code,
        MAX_BY("value", "load_date") as fx_rate
    FROM CARRY1ST_PLATFORM.RAW.BAMBOO_CARD_FX_RATE
    WHERE 1=1
        AND "base_currency" = 'USD'
        AND "load_date" = DATE(SYSDATE()) - 1
    GROUP BY 1
),

union_cte AS (
    SELECT
        DATE("load_date") AS prov_date,  
        18 AS c1st_prov_id,
        "id"::integer AS prov_sku_id, 
        'SEAGM' AS provider_name,
        "product_name" AS prov_prod_name,
        "name" AS prov_prod_name_2,
        "par_value" AS face_value,
        "par_value_currency" AS face_value_cy,
        "origin_price" AS srp,
        "currency" AS srp_cy,
        "unit_price" AS cogs,
        "currency" AS cogs_cy,
        IFF("currency" = 'USD', "unit_price", NULL) cogs_usd,
        "discount_rate" AS c1st_margin,
        'Discrete' AS volume_type,
        'Gift Card' AS product_type
    FROM CARRY1ST_PLATFORM.RAW.SEAGM_GIFT_CARD 

    UNION ALL

    SELECT
        DATE("load_date") AS prov_date,  
        18 AS c1st_prov_id,
        "id"::integer AS prov_sku_id, 
        'SEAGM' AS provider_name,
        "product_name" AS prov_prod_name,
        "name" AS prov_prod_name_2,
        "par_value" AS face_value,
        "par_value_currency" AS face_value_cy,
        "origin_price" AS srp,
        "currency" AS srp_cy,
        "unit_price" AS cogs,
        "currency" AS cogs_cy,
        IFF("currency" = 'USD', "unit_price", NULL) cogs_usd,
        "discount_rate" AS c1st_margin,
        'Discrete' AS volume_type,
        'Top Up' AS product_type
    FROM CARRY1ST_PLATFORM.RAW.seagm_top_up

    UNION ALL
-- EZPIN
    SELECT
        DATE("load_date") AS prov_date,                  
        14 AS c1st_prov_id,                              
        "sku"::integer AS prov_sku_id,                   
        'EZPIN' AS provider_name,                        
        MIN_BY("title", "min_price") AS prov_prod_name,                       
        MIN_BY("title", "min_price") AS prov_prod_name_2,                     
        MIN("min_price") AS face_value,                  
        "currency.code" AS face_value_cy,                
        IFF(MAX("percentage_of_buying_price") < 0, MIN("min_price"), NULL) AS srp,                                     
        IFF(MAX("percentage_of_buying_price") < 0, "currency.code", NULL) AS srp_cy,                                  
        MIN("min_price") * (1 + MAX("percentage_of_buying_price") / 100) AS cogs, 
        "currency.code" AS cogs_cy,                      
        IFF("currency.code" = 'USD', MIN("min_price") * (1 + MAX("percentage_of_buying_price") / 100), NULL) AS cogs_usd,
        MAX("percentage_of_buying_price") * -1 AS c1st_margin,
        IFF(MIN("min_price") = MIN("max_price"), 'Discrete', 'Continuous') AS volume_type,
        NULL AS product_type
    FROM CARRY1ST_PLATFORM.RAW.EZ_PIN_PRICES
    WHERE 1=1
        AND "currency.code" <> 'EZD'
    GROUP BY 1, 2, 3, 4, 8, 12
    
    UNION ALL
-- REWARD STORE
    SELECT
        prov_date AS prov_date,                  
        31 AS c1st_prov_id,                      
        sku_id::integer AS prov_sku_id,                   
        'REWARD STORE' AS provider_name,         
        name AS prov_prod_name,                  
        name AS prov_prod_name_2,               
        MIN(min_price) AS face_value,                    
        currency AS face_value_cy,              
        IFF(MIN(disc) > 0, MIN(min_price), NULL) AS srp,
        currency AS srp_cy,                     
        MIN(min_price) * (1 - MIN(disc) / 100) AS cogs,
        currency AS cogs_cy,                     
        IFF(currency = 'USD', MIN(min_price) * (1 - MIN(disc) / 100), NULL) AS cogs_usd,
        MIN(disc) AS c1st_margin,
        IFF(MIN(min_price) = MIN(max_price), 'Discrete', 'Continuous') AS volume_type,
        category AS product_type                 
    FROM reward_base
    GROUP BY 1, 2, 3, 4, 5, 6, 8, 10, 12, 16

    UNION ALL

    -- WG Cards
     SELECT 
        load_date AS prov_date,                  
        999997 AS c1st_prov_id,                              
        sku_id::integer AS prov_sku_id,                   
        'WG Cards' AS provider_name,                        
        item_name AS prov_prod_name,                       
        sku_name AS prov_prod_name_2,                     
        min_face_value AS face_value,                    
        currency_code AS face_value_cy,                  
        NULL AS srp,
        NULL AS srp_cy,
        min_price AS cogs,
        sku_price_currency AS cogs_cy,                        
        IFF(sku_price_currency = 'USD', min_price, NULL) AS cogs_usd,
        IFF(currency_code = sku_price_currency, ((min_face_value - min_price) / min_face_value) * 100, NULL ) AS c1st_margin,
        IFF(min_face_value = max_face_value, 'Discrete', 'Continuous') AS volume_type,
        CASE 
            WHEN REGEXP_LIKE(item_name, '.*top[- ]up.*', 'i') THEN 'Top Up' 
            WHEN REGEXP_LIKE(item_name, '.*gift[- ]card.*', 'i') THEN 'Gift Card'
            ELSE 'Unknown'
            END AS product_type   
    FROM carry1st_platform.raw.wg_cards_prices_v2 
    WHERE 1=1

    UNION ALL

    -- BAMBOO
    SELECT 
        b.prov_date,                  
        999998 AS c1st_prov_id,                              
        b.id AS prov_sku_id,                   
        'BAMBOO' AS provider_name,                       
        b.global_name AS prov_prod_name,                       
        b.sku_name AS prov_prod_name_2,                     
        b.min_face_value AS face_value,                    
        b.item_currency AS face_value_cy,                  
        NULL AS srp,
        NULL As srp_cy,
        b.price_min AS cogs,
        b.currency_code AS cogs_cy,                        
        IFF(
            b.currency_code = 'USD', 
            b.price_min, 
            b.price_min * f.fx_rate
            ) AS  cogs_usd,
        IFF(b.item_currency = b.currency_code, ((b.min_face_value - b.price_min) / b.min_face_value) * 100, NULL ) AS c1st_margin,
        IFF(b.min_face_value = b.max_face_value, 'Discrete', 'Continuous') AS volume_type,
        'Unknown' AS product_type   
    FROM bamboo_cte AS b
    LEFT JOIN bamboo_fx_cte AS f
        ON b.currency_code = f.currency_code
    WHERE 1=1
    
    UNION ALL
   -- GAMERSMARKET
    SELECT 
        DATE("load_date") AS prov_date,
        26 AS c1st_prov_id,
        "id" AS prov_sku_id,
        'GAMERSMARKET' AS provider_name,
        "platformName" AS prov_name_2,
        "productName" AS prov_name,
        "faceValue" AS face_value,                    
        NULL AS face_value_cy,                  
        NULL AS srp,
        NULL As srp_cy,
        "usdPrice" AS cogs,
        'USD' AS cogs_cy,                        
        "usdPrice" AS  cogs_usd,
        null AS c1st_margin,
        'Discrete' AS volume_type,
        'Gift Card' AS product_type   
    FROM carry1st_platform.raw.gamers_market_prices
    WHERE 1=1
        AND lower("platformName") <> 'test'

   --  -- ARTIFICIAL COGS
   --  SELECT 
   --      provider_id AS c1st_prov_id,
   --      provider_name AS provider_name,
   --      DATE('2025-11-06') AS prov_date,
   --      bundle_id AS prov_sku_id,
   --      value AS srp,
   --      null As srp_currency,
   --      (value * (1-(provider_discount/100))) AS usd_cogs,
   --      bundle_name AS prov_name,
   --      bundle_name AS prov_name_2,
   --      provider_discount AS prov_margin,
   --      currency AS prov_currency,
   --      'Unknown' AS volume_type,
   --      IFF(provider_id = 37,'Unknown','Gift Card') AS product_type,
   --      IFF(provider_id = 37,0,1) AS shop_used_flag,
   --      NULL AS provider_sku_category
   --  FROM carry1st_platform.refined.upload__artificial_cogs
)

SELECT
    prov_date,                  
    c1st_prov_id,                              
    prov_sku_id,                   
    provider_name,                        
    prov_prod_name,                       
    prov_prod_name_2,                     
    ROUND(face_value,2) AS face_value,                    
    face_value_cy,                  
    ROUND(srp,2) AS srp,
    srp_cy,
    ROUND(cogs,2) AS cogs,
    cogs_cy,                        
    ROUND(cogs_usd,2) AS cogs_usd,
    c1st_margin,
    volume_type,
    product_type       
FROM union_cte
WHERE 1=1
    AND prov_date <= current_date()-1;