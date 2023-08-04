
{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["customer_id"]
         ,cluster_by=["customer_id"]
    )
}}

WITH google_ads_accounts AS (

SELECT  customer_id --*
        ,MAX(customer_descriptive_name) AS customer_descriptive_name
        ,MAX(customer_time_zone)        AS customer_time_zone
        ,MAX(customer_currency_code)    AS customer_currency_code
        ,MAX(customer_manager)          AS customer_manager
        ,MAX(_airbyte_emitted_at)       AS ingestion_datetime_at
FROM bettrads-develop.landing_{{ var('BQ_DATASET') }}.ads_accounts
GROUP BY customer_id

)
SELECT *
FROM google_ads_accounts

