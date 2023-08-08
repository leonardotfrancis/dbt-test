
{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["day", "account_id", "campaign_id", "adset_id", "ad_id"]
         ,cluster_by=["day",  "account_id", "campaign_id", "ad_id"]
    )
}}

WITH facebook_ads_ads AS (

SELECT  *
FROM {{ var('BQ_PROJECT') }}.raw_{{ var('BQ_DATASET') }}.facebook_ads_ads 

), ads_age_gender as (
SELECT  age_genders.account_id 
        ,age_genders.campaign_id
        ,age_genders.adset_id 
        ,ARRAY_AGG( DISTINCT age_genders.gender) as gender
        ,ARRAY_AGG( DISTINCT age_genders.age) as age
FROM {{ var('BQ_PROJECT') }}.raw_{{ var('BQ_DATASET') }}.facebook_ads_age_and_gender  as age_genders
GROUP BY age_genders.account_id 
        ,age_genders.campaign_id
        ,age_genders.adset_id 
), ads_region as (
SELECT  regions.account_id 
        ,regions.campaign_id
        ,regions.adset_id 
        ,ARRAY_AGG( DISTINCT regions.region) as region
FROM {{ var('BQ_PROJECT') }}.raw_{{ var('BQ_DATASET') }}.facebook_ads_region  as regions
GROUP BY regions.account_id 
        ,regions.campaign_id
        ,regions.adset_id 
)
SELECT  ads.*, ads_age_gender.gender, ads_age_gender.age, ads_region.region
  FROM facebook_ads_ads as ads  LEFT JOIN ads_age_gender ON  ads_age_gender.account_id = ads.account_id 
                                                        AND ads_age_gender.campaign_id = ads.campaign_id
                                                        AND ads_age_gender.adset_id = ads.adset_id
                                LEFT JOIN ads_region    ON  ads_region.account_id = ads.account_id 
                                                        AND ads_region.campaign_id = ads.campaign_id
                                                        AND ads_region.adset_id = ads.adset_id

