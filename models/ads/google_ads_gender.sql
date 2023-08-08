
{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["day", "account_id", "campaign_id", "ad_group_id", "ad_id"]
         ,cluster_by=["day",  "account_id", "campaign_id", "ad_id"]
    )
}}

WITH google_ads_gender AS (

SELECT  *
FROM {{ var('BQ_PROJECT') }}.raw_{{ var('BQ_DATASET') }}.ads_google_ads_gender 

)
SELECT  *
  FROM google_ads_gender
