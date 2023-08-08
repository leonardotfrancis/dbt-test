
{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["day", "account_id", "campaign_id", "ad_group_id", "ad_id"]
         ,cluster_by=["day",  "account_id", "campaign_id", "ad_id"]
    )
}}

WITH google_ads_ads AS (

SELECT  *
FROM {{ var('BQ_PROJECT') }}.raw_{{ var('BQ_DATASET') }}.google_ads_ads 

), ads_gender as (
SELECT  ads_gender.account_id 
        ,ads_gender.campaign_id
        ,ads_gender.ad_group_id 
        ,ARRAY_AGG( DISTINCT ads_gender.gender) as gender
FROM {{ var('BQ_PROJECT') }}.raw_{{ var('BQ_DATASET') }}.google_ads_gender  as ads_gender
GROUP BY ads_gender.account_id 
        ,ads_gender.campaign_id
        ,ads_gender.ad_group_id 
)
SELECT  *
  FROM google_ads_ads as ads LEFT JOIN ads_gender ON ads_gender.account_id = ads.account_id 
                                                 AND ads_gender.campaign_id = ads.campaign_id
                                                 AND ads_gender.ad_group_id = ads.ad_group_id

