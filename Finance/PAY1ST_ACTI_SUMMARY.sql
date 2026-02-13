create or replace view CARRY1ST_PLATFORM.REFINED.PAY1ST_ACTI_SUMMARY as 

WITH base_cte As (
    SELECT 
        trx_date AS trx_date, 
        country AS country, 
       -- reference AS reference, 
        currency_code AS currency_cpde,
      --  product_id AS "C1st Product ID",
     --   bundle_id AS "C1st Bundle ID",
        bundle_name AS sku, 
        payment_method AS pay_method,
        chargeback_flag,
        SUM(item_qty) AS units,
        SUM(gmv_local) As gross_publisher_price_local,
        SUM(approved_discount_local) AS supported_discounts_local,     
        SUM(
            ((gmv_local - approved_discount_local) * IFF(country IN ('NG','ZA'), country_vat,0))
                / (1 + IFF(country IN ('NG','ZA'), country_vat,0))
            ) AS vat_local
    FROM carry1st_platform.refined.transaction_detail_s
    WHERE 1=1   
        AND reporting_flag = 'Yes'
        AND provider_name = 'Activision'
        AND trx_date >= current_date()-10
    GROUP BY 1,2,3,4,5,6
)

SELECT
    trx_date, 
    country, 
    currency_cpde,
    pay_method,
   -- "C1st Product ID",
  --  "C1st Bundle ID",
    sku, 
    units,
    
    ROUND(gross_publisher_price_local, 2) AS gross_publisher_price_local,
    ROUND(supported_discounts_local, 2) AS supported_discounts_local,
    ROUND(gross_publisher_price_local - supported_discounts_local,2) AS gross_revenue_local,

    ROUND(vat_local,2) AS vat_local,
    ROUND(IFF(chargeback_flag = 'Yes', gross_revenue_local -  vat_local,0),2) AS chargebacks_local,
    ROUND(gross_revenue_local - vat_local - chargebacks_local,2) AS net_revenue_local,

    ROUND((gross_revenue_local - vat_local - chargebacks_local) * 0.1,2) AS pay1st_revenue_local,
    ROUND((gross_revenue_local - vat_local - chargebacks_local) * 0.9,2) AS publisher_revenue_local
FROM base_cte
WHERE 1=1
