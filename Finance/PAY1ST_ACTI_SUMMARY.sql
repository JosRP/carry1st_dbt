--create or replace view CARRY1ST_PLATFORM.REFINED.PAY1ST_ACTI_SUMMARY as 
WITH base_cte As (
    SELECT 
        trx_date AS "Date of Transaction", 
        country AS "Country", 
     --   reference AS "C1st Transaction Reference", 
        currency_code AS "Currency Code",
      --  product_id AS "C1st Product ID",
     --   bundle_id AS "C1st Bundle ID",
        bundle_name AS "SKU", 
        payment_method AS "Payment Method",
        SUM(item_qty) AS "Units",
        ROUND(SUM(gmv_local),2) As "Gross Publisher Point Price (Local)",
        ROUND(SUM(approved_discount_local),2) AS "Supported Discounts (Local)",
        
        ROUND(SUM(
            ((nmv_local - chargeback_cost_local) * IFF(country IN ('NG','ZA'), country_vat,0))
            /
            (1 + IFF(country IN ('NG','ZA'), country_vat,0))
            ),2) AS "VAT (Local)",
        
        ROUND(SUM(chargeback_cost_local),2) AS "Chargebacks (Local)" 
    FROM carry1st_platform.refined.transaction_detail_s
    WHERE 1=1   
        AND reporting_flag = 'Yes'
        AND provider_name = 'Activision'
        AND trx_date >= current_date()-10
    GROUP BY 1,2,3,4,5
)

SELECT
    "Date of Transaction" AS trx_date, 
    "Country" AS country, 
    "Currency Code" AS currency_cpde,
   -- "C1st Transaction Reference", 
    "Payment Method" As pay_method,
   -- "C1st Product ID",
  --  "C1st Bundle ID",
    "SKU" As sku, 
    "Units" AS units,
    
    "Gross Publisher Point Price (Local)" AS gross_publisher_price_local,
    "Supported Discounts (Local)" AS supported_discounts_local,
    "Gross Publisher Point Price (Local)" - "Supported Discounts (Local)" AS gross_revenue_local,

    "VAT (Local)" AS vat_local,
    "Chargebacks (Local)" AS chargebacks_local,
    gross_revenue_local - "VAT (Local)" - "Chargebacks (Local)" AS net_revenue_local,

    (gross_revenue_local - "VAT (Local)" - "Chargebacks (Local)") * 0.1 AS pay1st_revenue_local,
    (gross_revenue_local - "VAT (Local)" - "Chargebacks (Local)") * 0.9 AS publisher_revenue_local
FROM base_cte
WHERE 1=1;