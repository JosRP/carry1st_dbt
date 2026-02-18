create or replace view CARRY1ST_PLATFORM.REFINED.T_GLOBAL_PAYMENTS as 

with pay_cte AS (
    SELECT 
        transaction_id,
        max_by(DEFAULT_METHOD_USED, created_date) AS DEFAULT_METHOD_USED,
        max_by(SAVED_METHOD_USED, created_date) AS SAVED_METHOD_USED,
        max_by(SAVED_METHOD_WITH_CREDENTIALS_USED, created_date) AS SAVED_METHOD_WITH_CREDENTIALS_USED    
    FROM CARRY1ST_PLATFORM.RAW.PAYMENT
    GROUP BY 1
),

cte AS (
    SELECT 
        email,
        integration_type,
        reference,
        country,
        country_slim,
        trx_date,
        payment_gateway,
        t.payment_method,
        COALESCE(CONCAT(UPPER(SUBSTR(payment_category, 1, 1)), LOWER(SUBSTR(payment_category, 2))), 'Missing') AS payment_category,
        COALESCE(CONCAT(UPPER(SUBSTR(payment_type, 1, 1)), LOWER(SUBSTR(payment_type, 2))), 'Missing') AS payment_type,
        device,
        device_version,
        classification,
        final_status,
        message,
        IFF(DEFAULT_METHOD_USED = True,'Yes', 'No') AS default_method,
        CASE 
            WHEN SAVED_METHOD_WITH_CREDENTIALS_USED = True THEN 'Saved w/ Creds'
            WHEN SAVED_METHOD_USED = True THEN 'Saved'
            ELSE'Not Saved'
            END AS saved_method,
        -- GROUPING BEGINS
        MAX_BY(CONCAT_WS('|', gate_source, flow_source, flow_partner, business_unit, COALESCE(provider_name, 'Unknown'), COALESCE(product_name, 'Unknown')), coalesce(gmv,0)) AS concat,
        SUM(IFF(reporting_flag = 'Yes', fiat_processed_amount, NULL)) AS fiat_processed_amount,
        LPAD(MIN(EXTRACT(HOUR FROM TO_TIMESTAMP_TZ(trx_datetime)))::STRING, 2, '0') AS trx_hour,
        CASE 
            WHEN MAX(reporting_flag) <> 'Yes' THEN NULL
            WHEN MAX(PAYMENT_COMPLETE_DATE) > MAX(FULFILMENT_COMPLETE_DATE) THEN NULL
            WHEN MAX(PAYMENT_COMPLETE_DATE) IS NULL THEN NULL
            WHEN MAX(FULFILMENT_COMPLETE_DATE) IS NULL THEN NULL
            ELSE ROUND(
                SUM(item_qty * TIMEDIFF('seconds', trx_datetime, PAYMENT_COMPLETE_DATE)) / SUM(item_qty),
                0)
            END AS pay_time_seconds,
        MIN(trx_date) OVER (PARTITION BY payment_gateway, message) AS first_seen_date
    FROM CARRY1ST_PLATFORM.REFINED.TRANSACTION_DETAIL_S AS t
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.payment_category_upload AS c
        ON t.payment_method = c.payment_method
    LEFT JOIN pay_cte AS p
        ON t.trx_id = p.transaction_id
    WHERE 1=1
        AND trx_date BETWEEN '2024-01-01' AND current_date() -1
        AND payment_gateway IS NOT NULL
        AND final_status IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

SELECT 
    c.email,
    c.reference,
    country,
    country_slim,
    trx_date,
    trx_hour,
    payment_gateway,
    payment_method,
    payment_category,
    payment_type,
    device,
    device_version,
    classification,
    final_status,
    message,
    integration_type,
    COALESCE(SPLIT_PART(concat, '|', 1), 'Unknown') AS gate_source,
    COALESCE(SPLIT_PART(concat, '|', 2), 'Unknown') AS flow_source,
    COALESCE(SPLIT_PART(concat, '|', 3), 'Unknown') AS flow_partner,
    COALESCE(SPLIT_PART(concat, '|', 4), 'Unknown') AS business_unit,
    COALESCE(SPLIT_PART(concat, '|', 5), 'Unknown') AS provider_name,
    COALESCE(SPLIT_PART(concat, '|', 6), 'Unknown') AS product_name,
    fiat_processed_amount,
    pay_time_seconds,
    IFF(c.trx_date = first_seen_date, 'Yes', 'No') AS is_first_message,
    CONCAT_WS('_',a.email,a.purchase_reference) AS email_purchase_attempt,
    a.purchase_successful,
    pc.chargeback_type,
    c.default_method,
    c.saved_method
FROM cte AS c
LEFT JOIN CARRY1ST_PLATFORM.REFINED.purchase_attempt AS a
    ON c.email = a.email
    AND c.reference = a.reference
LEFT JOIN carry1st_platform.refined.upload__payops_chargbacks AS pc
   ON  c.reference = pc.reference
