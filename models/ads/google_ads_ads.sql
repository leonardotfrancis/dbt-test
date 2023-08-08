
{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["segments_date", "customer_id", "campaign_id", "ad_group_id", "ad_group_ad_ad_id"]
         ,cluster_by=["segments_date",  "customer_id", "campaign_id", "ad_group_ad_ad_id"]
    )
}}

WITH google_ads_ads AS (

SELECT  ads.segments_date                       as day--*
        ,ads.customer_id                        as account_id --*
        -- ,ads.customer_descriptive_name          as account_name
        ,campaign_id --*
        ,campaign_name
        ,ad_group_id --*
        ,ad_group_name
        ,ad_group_ad_ad_id                      as ad_id --*
        ,null ad_name
        --campaign details
        ,campaign_start_date
        ,campaign_end_date
        ,campaign_advertising_channel_type                                                                              as campaign_channel
        ,campaign_payment_mode                                                                                          as campaign_payment_mode
        ,campaign_optimization_score                                                                                    as campaign_optimization_score
        ,ad_group_ad_ad_final_urls                                                                                      as ad_final_urls
        ,campaign_url_custom_parameters                                                                                 as campaign_url_custom_parameters
        --metrics               
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_cost_micros, 1000000),2), 0)                                                   as investment 
        ,IFNULL(ROUND(metrics_clicks,2), 0)                                                                              as clicks
        ,IFNULL(ROUND(metrics_impressions,2), 0)                                                                         as impressions 
        ,IFNULL(ROUND(metrics_interactions,2), 0)                                                                        as reacts
        ,IFNULL(ROUND(metrics_conversions,2), 0)                                                                         as conversions
        ,IFNULL(ROUND(metrics_engagements,2), 0)                                                                         as engagement   
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(metrics_engagements, metrics_impressions),1000),2), 0)                   as remarketing
        ,IFNULL(ROUND(metrics_video_views,2), 0)                                                                         as video_view
        ,IFNULL(ROUND(metrics_video_quartile_p25_rate,2), 0)                                                             as views_25
        ,IFNULL(ROUND(metrics_video_quartile_p50_rate,2), 0)                                                             as views_50
        ,IFNULL(ROUND(metrics_video_quartile_p75_rate,2), 0)                                                             as views_75
        ,IFNULL(ROUND(metrics_video_quartile_p100_rate,2), 0)                                                            as views_100
        ,IFNULL(ROUND(metrics_bounce_rate,2), 0)                                                                         as bounce_rate
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_engagements,metrics_impressions),2),0)                                         as engagement_rate   
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_conversions,metrics_clicks),2),0)                                              as conversions_rate 
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_average_cpm, 1000000),2), 0)                                                   as cpm
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_average_cpc, 1000000),2), 0)                                                   as cpc
        ,IFNULL(ROUND(SAFE_MULTIPLY(metrics_ctr,100),2), 0)                                                              as ctr
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_average_cpv, 1000000),4), 0)                                                   as cpv
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_cost_per_all_conversions, 1000000),4), 0)                                      as cpa
        ,IFNULL(ROUND(SAFE_DIVIDE(metrics_average_cpe, 1000000),4),0)                                                    as cpe
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(metrics_video_views, metrics_impressions),1000),2), 0)                   as vtr
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(metrics_video_quartile_p50_rate, metrics_impressions),1000),2), 0)       as vtr_50
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(metrics_video_quartile_p100_rate,metrics_impressions),1000),2), 0)       as vtr_100
        ,IFNULL(ROUND(metrics_top_impression_percentage,4),0)                                                            as topImpressionPercentage
        ,IFNULL(ROUND(metrics_absolute_top_impression_percentage,4),0)                                                   as absoluteTopImpressionPercentage
        ,_airbyte_ads_ad_group_ad_custom_hashid airbyte_hashid
        ,_airbyte_emitted_at ingestion_datetime_at
FROM {{ var('BQ_PROJECT') }}.landing_{{ var('BQ_DATASET') }}.ads_ad_group_ad_custom ads

)
SELECT  ads.day 
        ,ads.account_id 
        ,ads.account_name
        ,ads.campaign_id 
        ,ads.campaign_name
        ,ads.ad_group_id 
        ,ads.ad_group_name
        ,ads.ad_id 
        ,ads.ad_name
        ,ads.campaign_start_date  
        ,ads.campaign_end_date
        ,ads.campaign_channel 
        ,ads.campaign_payment_mode
        ,ads.campaign_optimization_score
        ,ads.ad_final_urls
        ,ads.campaign_url_custom_parameters
        ,ads.investment                                                                                                 as investment
        ,ads.impressions
        ,ads.frequency
        ,ads.reach
        ,IFNULL(ROUND(cpm,2), 0)                                                                                        as cpm
        ,IFNULL(ROUND(cpc,2), 0)                                                                                        as cpc  
        ,IFNULL(ROUND(ctr,2), 0)                                                                                        as ctr
        ,IFNULL(ROUND(cpv,2), 0)                                                                                        as cpv  
        ,IFNULL(ROUND(SAFE_DIVIDE(investment,conversions),2),0)                                                         as cpa
        ,IFNULL(ROUND(cpe,2), 0)                                                                                        as cpe
        ,IFNULL(clicks, 0)                                                                                              as clicks 
        ,IFNULL(conversions, 0)                                                                                         as conversions
        ,IFNULL(engagement, 0)                                                                                          as engagement
        ,IFNULL(reacts, 0)                                                                                              as reactions
        ,IFNULL(video_view, 0)                                                                                          as video_view
        ,IFNULL(views_25, 0)                                                                                            as views_25
        ,IFNULL(views_50, 0)                                                                                            as views_50
        ,IFNULL(views_75, 0)                                                                                            as views_75
        ,IFNULL(views_100, 0)                                                                                           as views_100
        ,IFNULL(remarketing,0)                                                                                          as remarketing
        ,IFNULL(engagement_rate,0)                                                                                      as engagement_rate 
        ,IFNULL(connect_rate,0)                                                                                         as connect_rate    
        ,IFNULL(vtr,0)                                                                                                  as vtr      
        ,IFNULL(vtr_50,0)                                                                                               as vtr_50      
        ,IFNULL(vtr_100,0)                                                                                              as vtr_100
        ,ingestion_datetime_at
     FROM google_ads_ads      as ads

