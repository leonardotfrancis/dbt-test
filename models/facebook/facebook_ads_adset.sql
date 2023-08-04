
{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["date_start", "account_id", "campaign_id", "adset_id", "publisher_platform", "platform_position", "impression_device" ]
         ,cluster_by=["date_start","campaign_id","adset_id"]
    )
}}


WITH facebook_ads_adset AS (

SELECT  day
        ,account_id 
        ,MAX(account_name)                                                              AS account_name
        ,campaign_id                                                    
        ,MAX(campaign_name)                                                             AS campaign_name
        ,adset_id                                                       
        ,MAX(adset_name)                                                                AS adset_name
        ,publisher_platform                                                     
        ,platform_position
        ,impression_device
        ,MAX(date_start)                                                                AS date_start
        ,MAX(date_stop)                                                                 AS date_stop                                                     
        ,MAX(optimization_goal)                                                         AS optimization_goal
        ,MAX(objective)                                                                 AS objective
        ,MAX(buying_type)                                                               AS buying_type
        ,SUM(investment)                                                                AS investment
        ,SUM(impressions)                                                               AS impressions
        ,AVG(frequency)                                                                 AS frequency
        ,SUM(reach)                                                                     AS reach
        ,SAFE_MULTIPLY(SAFE_DIVIDE(SUM(investment), SUM(impressions)), 1000)            AS cpm
        ,SAFE_DIVIDE(SUM(investment), SUM(downloads))                                   AS cpi    
        ,SAFE_DIVIDE(SUM(investment), SUM(clicks))                                      AS cpc  
        ,SAFE_MULTIPLY(SAFE_DIVIDE(SUM(clicks), SUM(impressions)), 100)                 AS ctr
        ,SAFE_DIVIDE(SUM(investment), SUM(video_view))                                  AS cpv  
        ,SAFE_DIVIDE(SUM(investment), SUM(conversions))                                 AS cpa
        ,SAFE_DIVIDE(SUM(investment), SUM(engagement))                                  AS cpe
        ,SUM(clicks)                                                                    AS clicks 
        ,SUM(conversion_all)                                                            AS conversion_all
        ,SUM(conversions)                                                               AS conversions
        ,SUM(engagement)                                                                AS engagement
        ,SUM(comments)                                                                  AS comments
        ,SUM(reactions)                                                                 AS reactions
        ,SUM(video_view)                                                                AS video_view
        ,SUM(lead)                                                                      AS lead
        ,SUM(downloads)                                                                 AS downloads
        ,SUM(orders)                                                                    AS orders
        ,SUM(views_25)                                                                  AS views_25
        ,SUM(views_50)                                                                  AS views_50
        ,SUM(views_75)                                                                  AS views_75
        ,SUM(views_100)                                                                 AS views_100
        ,SUM(visits)                                                                    AS visits
        ,SAFE_DIVIDE(SUM(investment), SUM(visits))                                      AS cp_visit    
        ,SAFE_DIVIDE(SUM(investment), SUM(lead))                                        AS cp_lead 
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(engagement),  SUM(impressions)), 1000),2)  AS remarketing     
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(engagement),  SUM(reach)      ), 1000),2)  AS engagement_rate 
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(conversions), SUM(visits)     ), 1000),2)  AS conversions_rate
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(visits),      SUM(clicks)     ), 1000),2)  AS connect_rate
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(video_view),  SUM(impressions)), 1000),2)  AS vtr
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(views_50),    SUM(impressions)), 1000),2)  AS vtr_50
        ,ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(SUM(views_100),   SUM(impressions)), 1000),2)  AS vtr_100
        ,SUM(fb_pixel_add_to_wishlist)                                                  AS fb_pixel_add_to_wishlist
        ,SUM(fb_pixel_lead)                                                             AS fb_pixel_lead
        ,SUM(fb_pixel_complete_registration)                                            AS fb_pixel_complete_registration
        ,SUM(fb_pixel_purchase)                                                         AS fb_pixel_purchase
        ,SUM(fb_pixel_add_payment_info)                                                 AS fb_pixel_add_payment_info
        ,SUM(fb_pixel_add_to_cart)                                                      AS fb_pixel_add_to_cart
        ,SUM(fb_pixel_custom)                                                           AS fb_pixel_custom
        ,SUM(fb_pixel_initiate_checkout)                                                AS fb_pixel_initiate_checkout
        ,SUM(fb_pixel_search)                                                           AS fb_pixel_search
        ,SUM(fb_pixel_view_content)                                                     AS fb_pixel_view_content
        ,MAX(ingestion_datetime_at)                                                     AS ingestion_datetime_at
FROM {{ ref('facebook_ads_ads') }}
GROUP BY date_start 
        ,account_id 
        ,campaign_id 
        ,adset_id 
        ,publisher_platform  
        ,platform_position
        ,impression_device 
)
SELECT *
FROM facebook_ads_adset

