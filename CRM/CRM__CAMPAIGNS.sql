--INSERT INTO carry1st_platform.refined.CRM__CAMPAIGNS 

SELECT 
    customer_id,
    last_recipient,
    push_token,
    variant_group,
    campaign_name,
    send_date,
   language_code,
   country_code,
    creation_sys_datetime
 FROM carry1st_platform.refined.crm_260218_ramadan_EG_new_account 