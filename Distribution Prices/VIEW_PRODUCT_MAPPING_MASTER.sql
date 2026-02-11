--create or replace view CARRY1ST_PLATFORM.REFINED.VIEW_PRODUCT_MAPPING_MASTER as

WITH source_data AS (
    SELECT 
        c1st_prov_id::integer AS c1st_provider_id,
        prov_sku_id::integer AS prov_sku_id,
        provider_name, 
        prov_prod_name AS prod_name_l1,
        prov_prod_name_2 AS prod_name_l2
    FROM carry1st_platform.refined.distribution_api__relevant_list
),
existing_mappings AS (
    SELECT 
        provider_id::integer As c1st_prov_id,
        prov_sku_id::integer As prov_sku_id,
        prod_map
    FROM CARRY1ST_PLATFORM.REFINED.DIST_PRICES_PROD_MAP
)
SELECT 
    s.c1st_provider_id,
    s.prov_sku_id,
    s.provider_name,
    s.prod_name_l1,
    s.prod_name_l2,
    m.prod_map AS current_mapping,
    CASE WHEN m.prod_map IS NOT NULL THEN TRUE ELSE FALSE END AS is_mapped
FROM source_data s
LEFT JOIN existing_mappings m 
    ON s.c1st_provider_id = m.c1st_prov_id 
    AND s.prov_sku_id = m.prov_sku_id;