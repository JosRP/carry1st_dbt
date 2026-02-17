--CREATE OR REPLACE VIEW carry1st_platform.refine.GA_SHOP_APP_SESSION_DETAIL AS

SELECT
    CONCAT_WS(
        '_', 
        user_pseudo_id, 
        event_params:ga_session_id::string) AS full_sid,
    user_pseudo_id AS ga_user_id,
    platform AS device,
    event_params:country_shop::string AS shop_country,
    event_date
    * 
FROM carry1st_platform.raw.event_dt
WHERE 1=1
    AND platform IN ('ANDROID', 'IOS')
    AND event_date = '2026-02-13'
LIMIT 10   



SELECT
    CONCAT_WS(
        '_', 
        user_pseudo_id, 
        event_params:ga_session_id::string) AS full_sid,
    max(user_id) AS user_id,
    max(user_properties:client_id::integer) As client_id,
    max(event_name = 'socials__sign_up_google__click'),
    max(event_name = 'purchase')
FROM carry1st_platform.raw.event_dt
WHERE 1=1
    AND platform IN ('ANDROID', 'IOS')
    AND event_date = '2026-02-13'
   -- AND full_sid = 'f8cf762449b0dcb49c97df75130a532c_1770983412'
   -- and full_sid = 'e71394aa73b758599de1d00f94d53b47_1770943741'
group by 1
  having 1=1 
  AND  max(user_id) is not null 
  and max(user_properties:client_id::integer)  is  null
  and max(event_name = 'purchase') = 0
  --and max(event_name = 'socials__sign_up_google__click') = 0

   limit 10



SELECT
    CONCAT_WS(
        '_', 
        user_pseudo_id, 
        event_params:ga_session_id::string) AS full_sid,
    user_id ,
    user_properties:client_id::integer As client_id,
    *
FROM carry1st_platform.raw.event_dt
WHERE 1=1
    AND platform IN ('ANDROID', 'IOS')
    AND event_date = '2026-02-13'
    AND full_sid = 'cf621469892a2d90c1e93dcfa8bc946c_1770991521'
    order by event_timestamp