CREATE OR REPLACE VIEW CARRY1ST_PLATFORM.REFINED.TRANSACTION_DETAIL_PRE1 AS

WITH payments_clean AS (
    SELECT 
        transaction_id,
        payment_channel_id
    FROM CARRY1ST_PLATFORM.RAW.PAYMENT
    WHERE 1=1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY transaction_id
        ORDER BY 
            CASE 
                WHEN payment_channel_id = 203 THEN 2
                ELSE 1
                END,
            payment_channel_id DESC
    ) = 1
),

attempts_cte AS (
    SELECT 
        t.country_code AS country,
        t.platform_version AS device_version,
        t.partner_source,
        CASE 
			WHEN t.platform IS NULL THEN 'Web'
			WHEN t.platform IN ('SHOP', 'WEB', 'SHOP_UAT') THEN 'Web'
			WHEN t.platform = 'AND' THEN 'Android'
			WHEN t.platform = 'IOS' THEN 'IOS'
			ELSE 'Error'
			END AS device,
        p.payment_channel_id AS payment_channel_id,
        UPPER(t.email) AS email,
        t.transaction_reference AS transaction_reference,
        IFF(partner_id = 5, t.external_reference, t.transaction_reference) AS reference,
        t.external_reference AS reference_external,
        DATE(t.created_date) AS trx_date,
        t.created_date AS trx_datetime, 
        po.name AS payment_gateway,
        ac.name AS payment_method,
        t.id AS trx_id,
        t.message,
        t.gateway_complete_date AS psp_datetime, 
        t.currency_code,
        IFF(partner_id IS NOT NULL, 'Liberty', 'Seamless') AS integration_type,
        CASE 
            WHEN UPPER(partner_source) = 'GARENA' THEN UPPER(partner_source)
            ELSE NULL
            END AS t_provider_name,
        CASE 
            WHEN UPPER(partner_source) = 'GARENA' THEN 'L1'
            ELSE NULL
            END AS t_provider_id,
        CASE 
            WHEN UPPER(partner_source) = 'GARENA' THEN UPPER(partner_source)
            ELSE NULL
            END AS t_product_name,
        CASE 
            WHEN UPPER(partner_source) = 'GARENA' THEN UPPER(partner_source)
            ELSE NULL
            END AS t_bundle_name,
        CASE 
            WHEN UPPER(partner_source) = 'GARENA' THEN fiat_amount
            ELSE NULL
            END AS t_fiat_amount, 
        exchange_rate AS t_fx_rate,
        max(CASE
            WHEN t.status IN ('SUCCESSFUL', 'CHARGEBACK', 'REFUND') THEN 'SUCCESSFUL'
            when t.status in ('PENDING') THEN 'PENDING'
            ELSE 'FAILED'
            END) AS final_status, 
        MAX(CASE 
                WHEN classification_code = 'TECHNICAL_FAILURE' THEN 'Technical Failure'
                WHEN classification_code = 'SUSPICIOUS_SUCCESSFUL' THEN 'Suspicious Successful'
                WHEN classification_code = 'REFUND' THEN 'Refund'
                WHEN classification_code = 'ABANDONED' THEN 'Abandoned'
                WHEN classification_code = 'SUCCESS' THEN 'Success'
                WHEN classification_code = 'PENDING' THEN 'Pending'
                WHEN classification_code = 'SUSPICIOUS_FAILED' THEN 'Suspicious Failed'
                WHEN classification_code = 'TESTING' THEN 'Testing'
                WHEN classification_code = 'USER_FAILURE' THEN 'User Failure'
                WHEN classification_code = 'BANK_FAILURE' THEN 'Bank Failure'
                WHEN classification_code IS NULL THEN NULL
                ELSE 'Not Coded'
                END) AS classification_new,
        MAX(CAST(rpc."classification_code" AS VARCHAR)) AS classification_code
    FROM CARRY1ST_PLATFORM.RAW.TRANSACTION  AS t
    LEFT JOIN payments_clean AS p 
        ON p.transaction_id = t.id
    LEFT JOIN CARRY1ST_PLATFORM.RAW.PAYMENT_CHANNEL AS pc 
        ON p.payment_channel_id = pc.id 
    LEFT JOIN CARRY1ST_PLATFORM.RAW.PAYMENT_OPTION AS po 
        ON pc.payment_option_id = po.id 
    LEFT JOIN CARRY1ST_PLATFORM.RAW.AVAILABLE_CHANNEL AS ac 
        ON  pc.channel_id = ac.id
    LEFT JOIN CARRY1ST_PLATFORM.RAW.REFERENCE_PAYMENT_CLASSIFICATION AS rpc 
        ON t.transaction_reference = rpc."transaction_reference" 
    WHERE 1=1
        AND p.transaction_id IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
    ),

attempts_cte_1 AS (    
    SELECT
        country,
        device_version,
        email,
        reference,
        reference_external,
        device,
        trx_date,
        trx_datetime,
        payment_gateway,
        payment_method,
        trx_id,
        c.message,
        final_status,     
        c.classification_code,
        psp_datetime,
        partner_source,
        payment_channel_id,
        t_provider_name,
        t_provider_id,
        t_product_name,
        t_bundle_name,
        t_fiat_amount,
        t_fx_rate,
        currency_code,
        COALESCE(classification_new ,COALESCE(p."category", 'Unclassified')) AS classification,
        integration_type,
        transaction_reference
    FROM attempts_cte AS c
    LEFT JOIN CARRY1ST_PLATFORM.RAW.REFERENCE_PAYMENT_CLASSIFICATION_CATEGORY AS p 
        ON c.classification_code = CAST(p."classification_code" AS VARCHAR)
    WHERE 1=1
        AND COALESCE(classification_new ,COALESCE(p."category", 'Unclassified')) NOT IN ('Refund')
    ),

po_cte AS (
    SELECT 
        DISTINCT 
        order_item_id, 
        IFF(stock_provider_name = 'Ezpin', 'EZPIN', stock_provider_name) AS stock_provider_name,
        CASE 
            WHEN stock_provider_name = 'Ezpin' THEN 14 
            WHEN stock_provider_name = 'GAMERSMARKET' THEN 26
            WHEN stock_provider_name = 'Reward Store' THEN 31
            WHEN stock_provider_name = 'Garena' THEN 999999
            ELSE NULL
            END AS stock_provider_id,
        CASE
            WHEN stock_provider_name = 'Garena' AND inventory_name LIKE '4%' THEN 0.04
            WHEN stock_provider_name = 'Garena' AND inventory_name LIKE '5.5%' THEN 0.055
            WHEN stock_provider_name = 'Garena' AND inventory_name LIKE '7%' THEN 0.07
            WHEN stock_provider_name = 'Garena' THEN 0.07
            WHEN stock_provider_name = 'GAMERSMARKET' AND DATE(REPLACE(created_date, 'Z ', '')) < '2023-12-01' THEN 0.073
            WHEN stock_provider_name = 'GAMERSMARKET' THEN 0.082
            ELSE 0 
            END AS po_perc,
        IFF(inventory_name LIKE '%EZPin_Undue_Orders_Batch%',1,0) AS undue_flag        
    FROM CARRY1ST_PLATFORM.RAW.FULFILMENT_TRANSACTION
    WHERE 1=1
        AND status = 'SUCCESSFUL'
        AND stock_provider_name IN ('Garena','GAMERSMARKET', 'Reward Store')
            OR inventory_name LIKE '%EZPin_Undue_Orders_Batch%'
    ),

product_name_cte AS (
	SELECT 
		DISTINCT
		"id" AS id,     
        REGEXP_REPLACE(FIRST_VALUE("name") OVER(PARTITION BY "id" ORDER BY "last_modified_date" ASC), '^[ \t]+|[ \t]+$', '') AS product_name
	FROM CARRY1ST_PLATFORM.REFINED.PRODUCT_NAME_CHANGE	
    ),

cdp_trx AS (
    SELECT 
        order_reference,
        MAX(ROUND(CASE WHEN cdp_type = 'Transaction Earned' THEN amount_usd ELSE 0 END,2)) AS cdp_usd_earned,
        MAX(ROUND(CASE WHEN cdp_type = 'Transaction Spent' THEN amount_usd ELSE 0 END,2)) AS cdp_usd_spent,
        MAX(ROUND(CASE WHEN cdp_type = 'Transaction Earned' THEN currency_amount_local ELSE 0 END,2)) AS cdp_local_earned,
        MAX(ROUND(CASE WHEN cdp_type = 'Transaction Spent' THEN currency_amount_local ELSE 0 END,2)) AS cdp_local_spent,
    FROM carry1st_platform.refined.cdp_transaction_detail
    WHERE 1=1
        AND cdp_type IN ('Transaction Earned', 'Transaction Spent')
        AND order_reference IS NOT NULL
    GROUP BY 1
    ),

pcl_cte AS (
    SELECT 
        id AS product_id, 
        MAX(partner_checkout_source) AS pcl_partner 
    FROM carry1st_platform.raw.product
    group by 1
    ),

gems_refund_cte AS (
    SELECT 
        note,
        ROUND(SUM(amount/1000),2) AS usd_amount
    FROM  CARRY1ST_PLATFORM.RAW.GEM_TRANSACTION
    WHERE 1=1
        AND status = 'REDEEMED'
        AND reversed = false
        AND allocation_type = 'REFUND'
    GROUP BY 1
    ),

order_cte AS (
    SELECT
        o.id AS order_id,
        o.created_date,
        o.reference, 
        TO_TIMESTAMP(REPLACE(i.fulfilment_complete_date, 'Z ', ''), 'YYYY-MM-DD HH24:MI:SS.FF') AS fulfilment_complete_date,
        TO_TIMESTAMP(REPLACE(i.payment_complete_date, 'Z ', ''), 'YYYY-MM-DD HH24:MI:SS.FF') AS payment_complete_date, 
        TO_TIMESTAMP(REPLACE(i.last_modified_date, 'Z ', ''), 'YYYY-MM-DD HH24:MI:SS.FF') AS last_modified_date, 
        COALESCE(d.stock_provider_name,pf.provider_name_fix, i.provider_name) AS provider_name, -- implement provider_fix here
        TO_VARCHAR(
            ROUND(
                COALESCE(
                        d.stock_provider_id,
                        pf.provider_id_fix, 
                        i.provider_id), 0)
            ) AS provider_id, -- implement provider_fix here
        o.distributor_id,
        o.partner_source,
        o.email,
        o.country_code,
        o.transaction_id,
        i.recipient_identifier As recipient_id,
        o.user_id AS user_id,
        i.product_id,
        pp.product_name AS product_name,
        i.product_bundle_id AS bundle_id,
        pb.name AS bundle_name,
        pb.external_id AS e_bundle_id,
        i.id As order_item_id,
        CASE 
            WHEN c."category" IS NULL THEN 'Unknown' 
            ELSE c."category" 
            END AS product_category,
        CASE 
            WHEN pp.product_name IN ('Gems', 'Carry1st Gems') THEN 'Yes' 
            ELSE 'No' 
            END AS gems_flag,            
        i.quantity AS item_qty,
        o.source AS utm_source,
        i.promo_code,
        IFF(i.promo_code IS NOT NULL, 'Yes', 'No') AS promo_code_flag,        
        i.discount_label,  
        IFF(i.discount_label IS NULL, 'No', 'Yes') As regular_flag,  
        IFF(i.discount_label IS NULL AND i.promo_code IS NULL, 'No', 'Yes') As discount_flag,
        CASE 
            WHEN o.payment_method = 'Free' THEN 'Yes' 
            ELSE 'No' 
            END AS free_flag,
        o.payment_gateway,
        o.payment_method,
        IFF(o.transaction_id IS NOT NULL, 'Yes', 'No') AS order_flag,
        o.currency_code,
        COALESCE(
            IFF(i.usd_exchange_rate = 0, NULL, i.usd_exchange_rate), 
            r.exchange_rate
            ) AS fx_rate,
        COALESCE(i.vip_points,0) AS vip_points,
        COALESCE((i.amount * i.quantity), 0) AS gmv_local,
        COALESCE((i.discount_amount * i.quantity), 0) As regular_discount_local,
        COALESCE((i.promo_code_discount_amount * i.quantity), 0) AS promo_code_discount_local,
        COALESCE((i.service_fee * i.quantity), 0) AS service_fee_local,
        COALESCE((i.convenience_fee * i.quantity), 0) AS convinience_fee_local,
        d.po_perc,

        DIV0(
            COALESCE(
                i.discount_amount * i.quantity,
                0) 
                +
            COALESCE(
                i.promo_code_discount_amount * i.quantity,
                0),
            COALESCE(
                i.amount * i.quantity,
                0
                )
            ) AS total_discount_weight,
        o.status As order_status,
        i.status AS item_status,
        COALESCE(d.undue_flag, 0) As undue_flag
    FROM CARRY1ST_PLATFORM.RAW.ORDERS as o
    LEFT JOIN CARRY1ST_PLATFORM.RAW.ORDER_ITEM as i
        ON o.id = i.order_id
    LEFT JOIN po_cte AS d
        ON  i.id = d.order_item_id
    LEFT JOIN product_name_cte AS pp
        ON i.product_id = pp.id
    LEFT JOIN CARRY1ST_PLATFORM.RAW.PRODUCT_BUNDLE AS pb
        ON i.product_id = pb.product_id
        AND i.product_bundle_id = pb.id
    LEFT JOIN CARRY1ST_PLATFORM.RAW.REFERENCE_AVG_COMMISSION_BY_PRODUCT AS c
        ON UPPER(TRIM(pp.product_name)) = UPPER(TRIM(c."product"))
        AND UPPER(TRIM(i.provider_name)) = UPPER(TRIM(c."provider"))
        AND o.created_date BETWEEN c."start_date" AND c."end_date"
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.FX_RATE_FIX_S AS r
        ON DATE(o.created_date) = r.trx_date
        AND o.currency_code = r.currency_code 
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.PSP_FEE_CALCULATION AS f
        ON o.transaction_id = f.trx_id
    LEFT JOIN carry1st_platform.refined.upload__provider_fix AS pf
        ON i.product_id = pf.product_id
    ),

log_cte AS (
  SELECT
    ORDER_ITEM_ID,
    MAX(CASE WHEN type <> 'REDEEM' OR type IS NULL THEN 1 ELSE 0 END) AS has_non_redeem
  FROM carry1st_platform.raw.ORDER_ITEM_LOG
  GROUP BY 1
),

trx_cte AS (
    SELECT  
        a.payment_channel_id,
        e.obs AS reference_exception,
        o.fulfilment_complete_date, 
        o.payment_complete_date, 
        o.last_modified_date, 
        a.message,
        a.transaction_reference,
        a.integration_type,
        IFF(sf.transaction_reference IS NOT NULL, 'SUCCESSFUL', a.final_status) As final_status,
        IFF(sf.transaction_reference IS NOT NULL, 'Success', a.classification) As classification,
        a.psp_datetime,
        a.device_version,
        COALESCE(a.t_provider_name, o.provider_name) AS provider_name,
        COALESCE(a.t_provider_id, TO_VARCHAR(ROUND(o.provider_id,0))) AS provider_id,
        COALESCE(a.t_product_name, o.product_name) AS product_name,
        COALESCE(a.t_bundle_name, o.bundle_name) AS bundle_name, 
        o.distributor_id,
        o.order_status,
        CASE 
            WHEN UPPER(pc.type) = UPPER('gate')
                THEN UPPER(COALESCE(a.t_provider_name, o.provider_name))
            WHEN o.distributor_id iS NOT NULL THEN 'Distributor'
            ELSE 'SHOP'
            END AS tech_gate_source,
        CASE 
            WHEN UPPER(pc.type) = UPPER('gate')
                THEN 'Gateway Partner'
            WHEN o.distributor_id iS NOT NULL THEN 'Distributor'
            ELSE 'SHOP' 
            END AS gate_source,     
        CASE 
            WHEN DATE(o.created_date) >= '2024-11-29' 
                AND pcl.pcl_partner IN ('PALMPAY', 'Nedbank') 
                THEN 'PCL'
            WHEN UPPER(pc.type) = UPPER('gate')
                THEN 'Gateway Partner'
            WHEN o.distributor_id iS NOT NULL THEN 'Flow1st' 
            ELSE 'Internal'
            END AS flow_source,
        CASE 
            WHEN UPPER(pc.type) = UPPER('gate')
                THEN UPPER(COALESCE(a.t_provider_name, o.provider_name))
            WHEN DATE(o.created_date) >= '2024-11-29' 
                AND pcl.pcl_partner = 'PALMPAY' 
                THEN 'PalmPay'
            WHEN DATE(o.created_date) >= '2024-11-29' 
                AND pcl.pcl_partner = 'Nedbank' 
                THEN 'Nedbank'
            WHEN o.distributor_id iS NOT NULL THEN d.NAME 
            ELSE 'Carry1st'
            END AS flow_partner,     
        CASE  
            WHEN pc.provider_id IS NULL THEN 'Retail'
            ELSE 'Pay1st'
            END AS business_unit,

        pc.comission AS pay1st_comission_perc,
        COALESCE(a.partner_source, o.partner_source) AS link_source,     
        COALESCE(a.device, 'Web') AS device,  
        COALESCE(a.trx_date, DATE(o.created_date)) AS trx_date,       
        COALESCE(a.trx_datetime, o.created_date) AS trx_datetime,       
        COALESCE(a.email, UPPER(o.email)) AS email,    
        COALESCE(a.country, o.country_code) As country,     
        g.country_slim AS country_slim,  
        COALESCE(a.trx_id, o.transaction_id) As trx_id,     
        COALESCE(a.reference, o.reference) AS reference,   
        a.reference_external,     
        CASE 
            WHEN o.distributor_id IS NOT NULL THEN 'Distributor'
            ELSE TRIM(COALESCE(a.payment_gateway, o.payment_gateway))
            END AS payment_gateway,  
        CASE 
            WHEN o.distributor_id IS NOT NULL THEN 'Distributor'
            ELSE TRIM(COALESCE(a.payment_method, o.payment_method))
            END AS payment_method,  
        o.recipient_id,    
        o.user_id,  
        CASE 
            WHEN o.user_id IS NOT NULL THEN 'Account' 
            ELSE 'No Account' 
            END AS account_flag,
        o.product_id, 
        o.bundle_id,
        o.e_bundle_id,
        o.order_item_id,
        o.product_category,
        o.gems_flag,
        o.item_qty,
        o.utm_source,
        o.promo_code,
        o.promo_code_flag,
        o.discount_label,
        o.regular_flag,
        o.discount_flag,     
        CASE 
            WHEN o.item_status IN('PARTIAL_REFUND', 'REFUND', 'CHARGEBACK', 'FAILED', 'ABANDONED', 'CANCELLED') 
                OR e.reference IS NOT NULL 
                OR gr.note IS NOT NULL
                THEN 'Yes' 
            ELSE 'No' 
            END AS chargeback_flag,
        o.free_flag,     
        CASE 
            WHEN o.item_status = 'SUCCESSFUL'
                AND COALESCE(a.trx_id, o.transaction_id) IS NULL 
                AND p.cdp_usd_spent > 0
                AND TRIM(COALESCE(a.payment_method, o.payment_method)) IS NULL
                THEN 'Yes'
            WHEN a.final_status = 'FAILED' 
                AND o.item_status = 'SUCCESSFUL' 
                AND (lg.ORDER_ITEM_ID IS NULL OR lg.has_non_redeem = 1)
                THEN 'Yes'
            WHEN a.t_provider_name IS NOT NULL AND a.final_status IN ('SUCCESSFUL') THEN 'Yes'
            WHEN sf.transaction_reference IS NOT NULL THEN 'Yes' 
            WHEN a.trx_id IS NOT NULL
                AND COALESCE(a.email, UPPER(o.email)) IS NOT NULL
                AND COALESCE(a.payment_gateway, o.payment_gateway) <> 'Free'
                and COALESCE(a.payment_method, o.payment_method) IS NOT NULL
                AND a.final_status IN ('SUCCESSFUL')
                AND o.item_status IS NOT NULL
                THEN 'Yes'
            WHEN o.distributor_id IS NOT NULL 
                AND o.item_status = 'SUCCESSFUL'
                and o.order_status = 'PAID'
                THEN 'Yes'
            ELSE 'No'
            END AS reporting_flag,    
        CASE 
           WHEN a.final_status = 'FAILED' 
                AND o.item_status = 'SUCCESSFUL' 
                AND (lg.ORDER_ITEM_ID IS NULL OR lg.has_non_redeem = 1)
                THEN 'Yes'
            WHEN o.item_status IN('PARTIAL_REFUND', 'REFUND', 'CHARGEBACK', 'FAILED', 'ABANDONED', 'CANCELLED') 
                OR e.reference IS NOT NULL 
                OR gr.note IS NOT NULL
                THEN 'No' 
            WHEN o.item_status = 'SUCCESSFUL' AND COALESCE(a.payment_method, o.payment_method) = 'Free'
                THEN 'Yes' 
            WHEN o.order_id IS NOT NULL
                AND o.item_status IN (
                    'SUCCESSFUL', 
                    'PARTIALLY_FULFILLED', 
                    'PARTIAL_REFUND', 
                    'REFUND', 
                    'CHARGEBACK')
                AND a.final_status <> 'FAILED'
                THEN 'Yes'
            WHEN o.distributor_id IS NOT NULL 
                AND o.item_status = 'SUCCESSFUL'
                and o.order_status = 'PAID'
                THEN 'Yes'
            ELSE 'No'
            END AS exchange_flag,    
        o.order_flag,  
        COALESCE(o.currency_code, a.currency_code) AS currency_code,
        COALESCE(o.fx_rate,a.t_fx_rate) AS fx_rate,
        o.vip_points,     
        COALESCE(a.t_fiat_amount, o.gmv_local) AS gmv_local, 
        COALESCE(o.regular_discount_local, 0) AS regular_discount_local,
        coalesce(o.promo_code_discount_local, 0) AS promo_code_discount_local,
        COALESCE(o.service_fee_local, 0) AS service_fee_local,
        COALESCE(o.convinience_fee_local, 0) AS convinience_fee_local,
        o.po_perc,
        o.total_discount_weight,
        o.item_status,
        o.undue_flag,
        CASE 
            WHEN t_provider_name IS NOT NULL THEN f.psp_fee_local
            ELSE (o.gmv_local / NULLIF(SUM(o.gmv_local) OVER (PARTITION BY o.reference), 0)) * COALESCE(f.psp_fee_local,0)
            END AS psp_fee_local,
        COALESCE((o.gmv_local / NULLIF(SUM(o.gmv_local) OVER (PARTITION BY o.reference), 0)) * COALESCE(p.cdp_usd_earned,0),0) AS cdp_usd_earned,
        COALESCE((o.gmv_local / NULLIF(SUM(o.gmv_local) OVER (PARTITION BY o.reference), 0)) * COALESCE(p.cdp_usd_spent,0),0) AS cdp_usd_spent,
        COALESCE((o.gmv_local / NULLIF(SUM(o.gmv_local) OVER (PARTITION BY o.reference), 0)) * COALESCE(p.cdp_local_spent,0),0) AS cdp_local_spent,

        IFF(gr.note IS NOT NULL,1,0) AS gems_refund_flag,
        COALESCE((o.gmv_local / NULLIF(SUM(o.gmv_local) OVER (PARTITION BY o.reference), 0)) * gr.usd_amount,0) AS gems_refund_usd

    --    (o.gmv_local / NULLIF(SUM(o.gmv_local) OVER (PARTITION BY o.reference), 0)) * COALESCE(f.api_fees,0) AS api_fees 
    FROM attempts_cte_1 AS a
    FULL JOIN order_cte AS o
        ON a.reference = o.reference
    LEFT JOIN carry1st_platform.refined.trx_exceptions_upload AS e
        ON COALESCE(a.reference, o.reference) = e.reference
    LEFT JOIN carry1st_platform.refined.dim_geo AS g
        ON COALESCE(a.country, o.country_code) = g.alpha_2_code
    LEFT JOIN pcl_cte AS pcl
        ON o.product_id = pcl.product_id
    LEFT JOIN carry1st_platform.refined.transaction_split_fix AS sf
        ON COALESCE(a.reference, o.reference) = sf.transaction_reference
    LEFT JOIN carry1st_platform.refined.upload__pay1st_comission AS pc
        ON COALESCE(a.t_provider_id, TO_VARCHAR(ROUND(o.provider_id,0))) = pc.provider_id
        AND COALESCE(a.trx_date, DATE(o.created_date)) BETWEEN pc.start_date AND pc.end_date
        AND 
            CASE 
                WHEN UPPER(a.partner_source) = 'ALLYSDK_GLOBAL' THEN 'gate'
                WHEN UPPER(COALESCE(a.t_provider_name, o.provider_name)) IN ('RIOT','MPL', 'GARENA') THEN 'gate'
                ELSE 'shop' END = pc.type
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.PSP_FEE_CALCULATION AS f
        ON COALESCE(a.trx_id, o.transaction_id) = f.trx_id
    LEFT JOIN cdp_trx AS p
        ON COALESCE(a.reference, o.reference) = p.order_reference 
    LEFT JOIN gems_refund_cte AS gr
        ON COALESCE(a.reference, o.reference) = gr.note
    LEFT JOIN log_cte AS lg
        ON o.ORDER_ITEM_ID = lg.ORDER_ITEM_ID
    LEFT JOIN CARRY1ST_PLATFORM.raw.DISTRIBUTOR AS d
        On o.distributor_id = d.id
    WHERE 1=1
    ),

dist_og_price_cte AS (
    SELECT 
        'Distributor' AS tech_gate_source,
        id AS bundle_id,
        product_id,
        price AS price_local,
        currency_code,
        valid_from,
        valid_to
    FROM carry1st_platform.refined.dist_og_price_view
),
    
calculations_cte AS (
    SELECT
        reference_exception,
        fulfilment_complete_date, 
        payment_complete_date, 
        last_modified_date, 
        item_status,
        distributor_id, 
        order_status,

    
        t.message,
        final_status,
        classification,
        psp_datetime,
        recipient_id,
        
        link_source,
        transaction_reference,
        integration_type,
        t.tech_gate_source,
        gate_source,
        flow_source,
        flow_partner,
        business_unit,

        t.provider_name,
        t.provider_id,

        device,
        device_version,
        t.trx_date,
        trx_datetime,
        email,
        t.country,
        country_slim,
        trx_id,
        reference,
        reference_external,
        payment_channel_id,
        payment_gateway,
        payment_method,
        
        user_id,
        account_flag,
        
        t.product_id,
        t.product_name,
        t.bundle_name,
        t.bundle_id,
        e_bundle_id,
        order_item_id,
        product_category,
        gems_flag,
        item_qty,

        utm_source,

        promo_code,
        promo_code_flag,
        discount_label,
        regular_flag,
        discount_flag,
        
        chargeback_flag,
        free_flag,
        reporting_flag,    
        order_flag,
        t.currency_code,
        fx_rate,

        vip_points,
        gmv_local As gmv_local_og,
        IFF(t.tech_gate_source = 'Distributor' 
                AND business_unit = 'Pay1st',
            (og.price_local / fx.exchange_rate) * fx_rate,
            gmv_local
            ) AS gmv_local,
        IFF(
            t.tech_gate_source IN ('Distributor')  
                AND UPPER(t.provider_name) = UPPER('Activision'),
            COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * pay1st_comission_perc,
            regular_discount_local
            ) AS regular_discount_local,
            
        promo_code_discount_local,
        service_fee_local,
        convinience_fee_local,
        psp_fee_local,
        exchange_flag,

        undue_flag,


        CASE 
            WHEN UPPER(t.provider_name) = UPPER('Activision') 
                THEN COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * (1 - COALESCE(ap.discount,0))
            ELSE 0 
            END AS partner_gmv_local,

        CASE 
            WHEN  UPPER(t.provider_name) = UPPER('Activision') 
                THEN COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * (1 - COALESCE(ap.discount,0)) * (1 - pay1st_comission_perc)
            ELSE 0 
            END AS partner_revenue_local,

        CASE 
            WHEN business_unit IN ('Retail') THEN 0
            WHEN gate_source = 'Gateway Partner' 
                AND UPPER(t.provider_name) IN ('MPL', 'RIOT') 
                THEN pay1st_comission_perc * gmv_local + COALESCE(psp_fee_local,0)
            WHEN gate_source = 'Gateway Partner' 
                AND UPPER(t.provider_name) IN ('NETEASE GAMES', 'GARENA')
                THEN pay1st_comission_perc * (gmv_local - regular_discount_local - promo_code_discount_local) + COALESCE(psp_fee_local,0)
            WHEN gate_source IN ('SHOP', 'Distributor') 
                AND UPPER(t.provider_name) IN ('NANOBIT', 'BOOMPLAY', 'SUPERCELL', 'TIMWE GROUP', 'BIG FISH GAMES') 
                THEN pay1st_comission_perc * (COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) - regular_discount_local - promo_code_discount_local) 
            WHEN gate_source IN ('SHOP', 'Distributor') 
                AND UPPER(t.provider_name) = UPPER('Activision') 
                THEN COALESCE((og.price_local / fx.exchange_rate) * (1-ap.discount) * fx_rate, gmv_local) * (1 - LEAST(COALESCE(ap.discount,0), total_discount_weight)) * pay1st_comission_perc
            WHEN gate_source IN ('SHOP', 'Distributor')  
                AND UPPER(t.provider_name) = UPPER('EA Sports') 
                THEN pay1st_comission_perc * (COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * (1 - x.value)) 
            WHEN gate_source IN ('SHOP', 'Distributor') 
                AND UPPER(t.provider_name) = UPPER('Audiomack') 
                THEN COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * (1 - LEAST(COALESCE(ap.discount,0), total_discount_weight)) * pay1st_comission_perc
            WHEN gate_source IN ('SHOP', 'Distributor') 
                AND UPPER(t.provider_name) = UPPER('Netease Games')
                THEN COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * (1 - LEAST(COALESCE(ap.discount,0), total_discount_weight)) * pay1st_comission_perc + COALESCE(psp_fee_local,0)
            END AS pay1st_comission_local,
        
        x.value AS country_vat,

        IFF(
            (business_unit = 'Retail' OR UPPER(t.provider_name) NOT IN (UPPER('Activision'), UPPER('Audiomack'))),
            0,
            (gmv_local * GREATEST(total_discount_weight - COALESCE(ap.discount,0), 0))
            )  AS pay1st_disc_cost_local,

        NULL AS margin_perc,   
        CASE
            WHEN 
                chargeback_flag = 'Yes' 
                OR business_unit = 'Pay1st'
                OR gmv_local IS NULL
                THEN 0 
            WHEN cf.COGS_SOURCE = 'API' THEN (cf.API_usd_cogs * item_qty * fx_rate)
            WHEN cf.COGS_SOURCE = 'DIRECT' THEN (cf.GSHEET_USD_COGS * item_qty * fx_rate)
            WHEN cf.COGS_SOURCE = 'BUNDLE' THEN gmv_local * (1 - COALESCE(cf.bundle_perc, 0)) 
            WHEN cf.COGS_SOURCE = 'PRODUCT' THEN gmv_local * (1 - COALESCE(cf.product_perc, 0)) 
            ELSE gmv_local
            END AS cogs_local,

        COALESCE((og.price_local / fx.exchange_rate) * fx_rate, gmv_local) * COALESCE(ap.discount,0) AS approved_discount_local,

        IFF(
            t.tech_gate_source IN ('Distributor'),
            0,
            gmv_local 
                - regular_discount_local 
                - promo_code_discount_local 
                + service_fee_local 
                + convinience_fee_local 
         ) AS processed_amount_local,

        CASE
            WHEN t.tech_gate_source = 'Distributor' THEN 0
            WHEN gems_flag = 'Yes' THEN 0
            WHEN chargeback_flag = 'Yes' THEN gmv_local - regular_discount_local - promo_code_discount_local + service_fee_local + convinience_fee_local
            ELSE 0
            END AS chargeback_cost_local,    

        CASE 
            WHEN t.tech_gate_source = 'Distributor' AND UPPER(t.provider_name) = UPPER('Activision')
                THEN (COALESCE(og.price_local,0) / fx.exchange_rate) * fx_rate * (1-COALESCE(ap.discount,0))
            ELSE gmv_local - regular_discount_local - promo_code_discount_local
            END AS nmv_local,

        CASE 
            WHEN business_unit = 'Pay1st' THEN 0
            WHEN gems_flag = 'Yes' THEN (-1) * (regular_discount_local + promo_code_discount_local)
            ELSE gmv_local - regular_discount_local - promo_code_discount_local
            END AS retail_nmv_local,

        IFF(gems_flag = 'Yes', 0, item_qty) AS item_qty_n_gems,
        IFF(gems_flag = 'Yes', NULL, trx_id) AS trx_id_n_gems,
        IFF(gems_flag = 'Yes', NULL, reference) AS reference_n_gems,
        IFF(gems_flag = 'Yes', NULL, email) AS email_n_gems,
        IFF(gems_flag = 'Yes', NULL, recipient_id) AS recipient_id_n_gems,
        IFF(gems_flag = 'Yes', NULL, user_id) AS user_id_n_gems,
        IFF(gems_flag = 'Yes', 0, gmv_local) AS gmv_n_gems_local,

        cdp_usd_earned,
        cdp_usd_spent,
        cdp_local_spent,
        ffx.c1st_rate AS finance_fx,

        gems_refund_flag,
        gems_refund_usd,

        IFF(
            t.tech_gate_source <> 'Distributor' OR business_unit <> 'Pay1st', 
            0, 
            COALESCE(
                ((og.price_local / fx.exchange_rate) * fx_rate) * (1-COALESCE(ap.discount,0))  - gmv_local,
                0)) AS mktg_dist_costs 
    FROM trx_cte AS t
    LEFT JOIN carry1st_platform.refined.cogs__full AS cf
        ON  TO_VARCHAR(t.provider_id)  = TO_VARCHAR(ROUND(cf.provider_id,0)) 
        AND TO_VARCHAR(ROUND(t.product_id,0))  = TO_VARCHAR(ROUND(cf.product_id,0)) 
        AND TO_VARCHAR(ROUND(t.bundle_id,0))  = TO_VARCHAR(ROUND(cf.bundle_id,0))
        AND t.trx_date = cf.calendar_date
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.upload__taxes AS x
        ON t.country = x.country 
        AND trx_date BETWEEN DATE(x.start_date) AND DATE(x.end_date)
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.upload__finance_fx AS ffx
        ON t.currency_code = ffx.currency_code
        AND t.trx_date BETWEEN ffx.start_date AND ffx.end_date
    LEFT JOIN carry1st_platform.refined.upload__approved_discounts AS ap
        ON CAST(t.provider_id AS VARCHAR)= CAST(ap.provider_id AS VARCHAR)
        AND CAST(t.product_id AS VARCHAR) = CAST(ap.product_id AS VARCHAR)
        AND CAST(t.bundle_id AS VARCHAR) = CAST(ap.bundle_id AS VARCHAR)
        AND t.trx_date BETWEEN ap.start_date and ap.end_date 
    LEFT JOIN dist_og_price_cte AS og
        ON t.bundle_id = og.bundle_id
        and t.tech_gate_source = og.tech_gate_source
        and t.trx_date BETWEEN og.valid_from and og.valid_to
    LEFT JOIN carry1st_platform.refined.fx_rate_fix_s AS fx
        ON t.trx_date = fx.trx_date
        AND og.currency_code = fx.currency_code
    ),
 
group_cte AS (
    SELECT
        fulfilment_complete_date, 
        payment_complete_date, 
        last_modified_date,
        reference_exception,
        item_status,
        distributor_id,
        order_status,
    
        c.message,
        final_status,
        CASE 
            WHEN classification = 'Unclassified' 
                AND payment_method = 'Carry1st Discount Points'
                AND final_status = 'SUCCESSFUL'
                THEN 'Success'
            WHEN classification = 'Unclassified' 
                AND payment_method = 'Carry1st Discount Points'
                AND final_status = 'FAILED'
                THEN 'Technical Failure'
            ELSE classification
            END AS classification,
        psp_datetime,
        recipient_id,

        link_source,
        tech_gate_source,
        gate_source,
        flow_source,
        flow_partner,
        business_unit,
        transaction_reference, 
        integration_type,

        provider_name,
        provider_id,
        device,
        device_version,
        c.trx_date,
        trx_datetime,
        IFF(tech_gate_source = 'Distributor', recipient_id, email) AS email,
        country,
        country_slim,
        trx_id,
        c.reference,
        reference_external,
        payment_channel_id,
        IFF(
            payment_gateway IS NULL 
                AND reporting_flag = 'Yes', 
            'Carry1st',
            payment_gateway) AS payment_gateway,
        IFF(
            payment_method IS NULL 
                AND reporting_flag = 'Yes', 
            'Carry1st Discount Points', 
            payment_method) AS payment_method,

        
        user_id,
        account_flag,
        
        product_id,
        product_name,
        bundle_name,
        c.bundle_id,
        e_bundle_id,
        order_item_id,
        product_category,
        gems_flag,
        
        utm_source,

        promo_code,
        promo_code_flag,
        discount_label,
        regular_flag,
        discount_flag,
        
        chargeback_flag,
        free_flag,
        reporting_flag,  
        order_flag,
        c.currency_code,  
        c.fx_rate AS fx_rate,

		trx_id_n_gems,
		reference_n_gems,
        email_n_gems,
		user_id_n_gems,
        recipient_id_n_gems,

        margin_perc,
        exchange_flag,
        undue_flag,
        country_vat,
        finance_fx,
        IFF(user_id = 1993996,1,0) AS offline_full_flag,

       -- SUM(api_fees) AS api_fees, -- placeholder (delete at will)(adjust group by)

        SUM(c.item_qty) AS item_qty,
        SUM(c.item_qty_n_gems) AS item_qty_n_gems,
        SUM(c.vip_points) AS vip_points,

        SUM(gmv_local_og) AS gmv_local_og,
        SUM(gmv_local) AS gmv_local,
        SUM(regular_discount_local) AS regular_discount_local,
        SUM(promo_code_discount_local) AS promo_code_discount_local,
        SUM(service_fee_local)  AS service_fee_local,
        SUM(convinience_fee_local)  AS convinience_fee_local,
        SUM(psp_fee_local) AS psp_fee_local,
        SUM(pay1st_comission_local) AS pay1st_comission_local,
        SUM(partner_gmv_local) AS partner_gmv_local,
        SUM(partner_revenue_local) AS partner_revenue_local,
        SUM(pay1st_disc_cost_local) AS pay1st_disc_cost_local,
        SUM(cogs_local) AS cogs_local,
        SUM(processed_amount_local) AS processed_amount_local,
        SUM(cdp_usd_spent * c.fx_rate) AS cdp_processed_amount_local,
        SUM(processed_amount_local) - SUM(cdp_local_spent) AS fiat_processed_amount_local,
        SUM(
            CASE 
                WHEN gems_flag = 'Yes' THEN 0
                WHEN gems_refund_flag = 1 THEN gems_refund_usd * c.fx_rate
                ELSE chargeback_cost_local
                END) AS chargeback_cost_local,
        SUM(nmv_local) AS nmv_local,
        SUM(retail_nmv_local) AS retail_nmv_local,	    
		SUM(gmv_n_gems_local) AS gmv_n_gems_local,
        (SUM(gmv_n_gems_local) - SUM(regular_discount_local) - SUM(promo_code_discount_local)) AS nmv_n_gems_local,

        SUM(retail_nmv_local) 
            + SUM(convinience_fee_local) 
            + SUM(service_fee_local)
            + SUM(pay1st_comission_local)
            - SUM(
                CASE 
                    WHEN gems_flag = 'Yes' THEN 0
                    WHEN gems_refund_flag = 1 THEN gems_refund_usd * c.fx_rate
                    ELSE chargeback_cost_local
                    END)
            - SUM(cdp_usd_earned * c.fx_rate * 0.6)
            AS revenue_local,

        SUM(retail_nmv_local) 
            + SUM(convinience_fee_local) 
            + SUM(service_fee_local)
            + SUM(pay1st_comission_local)
            - SUM(
                CASE 
                    WHEN gems_flag = 'Yes' THEN 0
                    WHEN gems_refund_flag = 1 THEN gems_refund_usd * c.fx_rate
                    ELSE chargeback_cost_local
                    END)
            - SUM(cdp_usd_earned * c.fx_rate * 0.6)
            - SUM(psp_fee_local)
            - SUM(cogs_local)
            AS gp_local,
        SUM(mktg_dist_costs) AS mktg_dist_costs_local,

        SUM(gmv_local_og / c.fx_rate) AS gmv_og,
        SUM(gmv_local / c.fx_rate) AS gmv,
        SUM(regular_discount_local / c.fx_rate) AS regular_discount,
        SUM(promo_code_discount_local / c.fx_rate) AS promo_code_discount,
        SUM(service_fee_local / c.fx_rate) AS service_fee,
        SUM(convinience_fee_local / c.fx_rate) AS convinience_fee,
        SUM(psp_fee_local / c.fx_rate) AS psp_fee,
        SUM(pay1st_comission_local / c.fx_rate) AS pay1st_comission,
        SUM(partner_gmv_local / c.fx_rate) AS partner_gmv,
        SUM(partner_revenue_local / c.fx_rate) AS partner_revenue,
        SUM(pay1st_disc_cost_local / c.fx_rate) AS pay1st_disc_cost,
        SUM(cogs_local / c.fx_rate) AS cogs,
        SUM(processed_amount_local / c.fx_rate) AS processed_amount,
        SUM(cdp_usd_spent) AS cdp_processed_amount,
        SUM(processed_amount_local / c.fx_rate) - SUM(cdp_usd_spent) AS fiat_processed_amount,     
        SUM(
            CASE 
                WHEN gems_flag = 'Yes' THEN 0
                WHEN gems_refund_flag = 1 THEN gems_refund_usd 
                ELSE chargeback_cost_local / c.fx_rate
                END) AS chargeback_cost,
        SUM(nmv_local / c.fx_rate) AS nmv,
        SUM(retail_nmv_local / c.fx_rate) AS retail_nmv,	    
        SUM(gmv_n_gems_local / c.fx_rate) AS gmv_n_gems,
		(SUM(gmv_n_gems_local / c.fx_rate) - SUM(regular_discount_local / fx_rate) - SUM(promo_code_discount_local / fx_rate)) AS nmv_n_gems,
        
        SUM(psp_fee_local / fx_rate) AS psp_fee_local_gate,
        SUM(processed_amount_local / c.fx_rate) - SUM(cdp_usd_spent) * MAX(r.EXCHANGE_RATE) AS fiat_processed_amount_local_gate,
        SUM(processed_amount_local / fx_rate) * MAX(r.EXCHANGE_RATE) AS processed_amount_local_gate,
        
        SUM(cdp_usd_earned) AS cdp_usd_earned,
        SUM(cdp_usd_earned) * 0.6 AS cdp_usd_earned_fin,

        SUM(retail_nmv_local / c.fx_rate) 
            + SUM(convinience_fee_local / c.fx_rate) 
            + SUM(service_fee_local / c.fx_rate)
            + SUM(pay1st_comission_local / c.fx_rate)
            - SUM(
                CASE 
                    WHEN gems_flag = 'Yes' THEN 0
                    WHEN gems_refund_flag = 1 THEN gems_refund_usd 
                    ELSE chargeback_cost_local / c.fx_rate
                    END)
            - SUM(cdp_usd_earned * 0.6)
            AS revenue,

        SUM(retail_nmv_local / c.fx_rate) 
            + SUM(convinience_fee_local / c.fx_rate) 
            + SUM(service_fee_local / c.fx_rate)
            + SUM(pay1st_comission_local / c.fx_rate)
            - SUM(
                CASE 
                    WHEN gems_flag = 'Yes' THEN 0
                    WHEN gems_refund_flag = 1 THEN gems_refund_usd 
                    ELSE chargeback_cost_local / c.fx_rate
                    END)
            - SUM(cdp_usd_earned * 0.6)
            - SUM(psp_fee_local / c.fx_rate)
            - SUM(cogs_local / c.fx_rate)
            AS gp,


              SUM(approved_discount_local) AS approved_discount_local,
              
              SUM(mktg_dist_costs / c.fx_rate) AS mktg_dist_costs
    FROM calculations_cte AS c
    LEFT JOIN CARRY1ST_PLATFORM.REFINED.FX_RATE_FIX_S AS r
        ON c.trx_date = r.trx_date
        AND 
            CASE 
                WHEN c.payment_gateway = 'PayGenius' THEN 'ZAR' 
                WHEN c.payment_gateway = 'Paga' THEN 'NGN'
                ELSE NULL 
                END = r.currency_code
    --WHERE email IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10,
        11,12,13,14,15,16,17,18,19,20,
        21,22,23,24,25,26,27,28,29,30,
        31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,
        51,52,53,54,55,56,57,58,59,60,
        61,62,63,64,65,66,67
    )

SELECT
    *
FROM group_cte;