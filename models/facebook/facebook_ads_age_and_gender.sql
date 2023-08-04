
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental'
         ,incremental_strategy='merge'
         ,primary_key=["date_start", "account_id", "campaign_id", "adset_id", "ad_id", "age", "gender" ]
         ,cluster_by=["date_start","campaign_id","adset_id","ad_id"]
    )
}}

WITH facebook_ads_ads_age_and_gender AS (

SELECT  date_start day--*
        ,account_id --*
        ,account_name
        ,campaign_id --*
        ,campaign_name
        ,adset_id --*
        ,adset_name
        ,ad_id --*
        ,ad_name
        ,age --* 
        ,gender --* 
        --campaign details
        ,date_start
        ,date_stop
        ,optimization_goal
        ,objective
        ,buying_type
        --metrics
        ,spend
        ,impressions
        ,reach
        ,frequency
        ,cpc
        ,cpm
        ,ctr
        ,cpp
        ,inline_link_click_ctr
        ,_airbyte_facebook_ads_insights_age_and_gender_hashid airbyte_hashid
        ,_airbyte_emitted_at ingestion_datetime_at
FROM bettrads-develop.landing_{{ var('BQ_DATASET') }}.facebook_ads_insights_age_and_gender

)
,facebook_ads_open_columns AS (
-- Transformar Multiplas Linhas em Uma Coluna cada
SELECT  ads.airbyte_hashid
        ,MAX(if(actions.action_type = 'post_reaction',               actions.value, 0))      AS post_reaction
        ,MAX(if(actions.action_type = 'post',                        actions.value, 0))      AS post
        ,MAX(if(actions.action_type = 'link_click',                  actions.value, 0))      AS link_click
        ,MAX(if(actions.action_type = 'comment',                     actions.value, 0))      AS comments
        ,MAX(if(actions.action_type = 'page_engagement',             actions.value, 0))      AS page_engagement
        ,MAX(if(actions.action_type = 'post_engagement',             actions.value, 0))      AS post_engagement
        ,MAX(if(actions.action_type = 'photo_view',                  actions.value, 0))      AS photo_view
        ,MAX(if(actions.action_type = 'onsite_conversion.post_save', actions.value, 0))      AS onsite_conversion_post_save
        ,MAX(if(actions.action_type = 'post_view',                   actions.value, 0))      AS post_view
        ,MAX(if(actions.action_type = 'video_view',                  actions.value, 0))      AS video_view
        ,MAX(if(actions.action_type = 'lead',                        actions.value, 0))      AS lead
        ,MAX(if(actions.action_type = 'omni_app_install',            actions.value, 0))      AS omni_app_install
        ,MAX(if(actions.action_type = 'omni_complete_registration',  actions.value, 0))      AS omni_complete_registration
        ,MAX(if(actions.action_type = 'landing_page_view',           actions.value, 0))      AS landing_page_view
        ,SUM(actions.value)                                                                  AS conversion_all
        -- Soma de todos Actions Exceto os tipos que Não São Conversions
        ,SUM(if(actions.action_type = 'video_view', 1, 0))                                   AS conversions_duplicates_count
        ,SUM(if(actions.action_type not in('comment',				
                                            'video_view',	
                                            'like',				
                                            'link_click',			
                                            'outbound_click',		
                                            'photo_view',			
                                            'post',				
                                            'post_reaction',		
                                            'page_engagement',		
                                            'post_engagement'	
                                            ),(actions.value), 0))                          AS conversions
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_add_to_wishlist',        actions.value, 0))  AS fb_pixel_add_to_wishlist
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_lead',                   actions.value, 0))  AS fb_pixel_lead
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_complete_registration',  actions.value, 0))  AS fb_pixel_complete_registration
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_purchase',               actions.value, 0))  AS fb_pixel_purchase
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_add_payment_info',       actions.value, 0))  AS fb_pixel_add_payment_info
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_add_to_cart',            actions.value, 0))  AS fb_pixel_add_to_cart
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_custom',                 actions.value, 0))  AS fb_pixel_custom
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_initiate_checkout',      actions.value, 0))  AS fb_pixel_initiate_checkout
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_search',                 actions.value, 0))  AS fb_pixel_search
        ,MAX(if(actions.action_type = 'offsite_conversion.fb_pixel_view_content',           actions.value, 0))  AS fb_pixel_view_content

        ,MAX(if(cost_action.action_type = 'omni_app_install'    ,cost_action.value, 0))     AS cpi
        ,MAX(if(cost_action.action_type = 'link_click'          ,cost_action.value, 0))     AS cpc
        ,MAX(if(cost_action.action_type = 'page_engagement'     ,cost_action.value, 0))     AS cpe
        ,MAX(if(cost_action.action_type = 'landing_page_view'   ,cost_action.value, 0))     AS cp_visit
        ,MAX(if(cost_action.action_type = 'lead'                ,cost_action.value, 0))     AS cp_lead

        ,MAX(if(video_p25.action_type  = 'video_view',video_p25.value,  0))                 AS video_p25  --video_p25_watched_actions
        ,MAX(if(video_p50.action_type  = 'video_view',video_p50.value,  0))                 AS video_p50  --video_p50_watched_actions
        ,MAX(if(video_p75.action_type  = 'video_view',video_p75.value,  0))                 AS video_p75  --video_p75_watched_actions
        ,MAX(if(video_p100.action_type = 'video_view',video_p100.value, 0))                 AS video_p100 --video_p100_watched_actions

        ,MAX(if(cost_per_thruplay.action_type = 'video_view',cost_per_thruplay.value, 0))   AS cpv        --cost_per_thruplay

        ,MAX(if(unique_video.action_type    = 'video_view',         actions.value,0))       AS views_3s  -- unique_video_continuous_2_sec_watched_actions
        ,MAX(if(video.action_type           = 'video_view',         actions.value,0))       AS true_view -- video_thruplay_watched_actions
        ,MAX(if(unique_actions.action_type  = 'omni_add_to_cart',   actions.value,0))       AS add_to_cart --video_p100_watched_actions
FROM    (SELECT airbyte_hashid 
           FROM facebook_ads_ads_age_and_gender
        GROUP BY airbyte_hashid) ads
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_actions                                   actions             ON ads.airbyte_hashid = actions._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_cost_per_action_type                      cost_action         ON ads.airbyte_hashid = cost_action._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_video_p25_watched_actions                 video_p25           ON ads.airbyte_hashid = video_p25._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_video_p50_watched_actions                 video_p50           ON ads.airbyte_hashid = video_p50._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_video_p75_watched_actions                 video_p75           ON ads.airbyte_hashid = video_p75._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_video_p100_watched_actions                video_p100          ON ads.airbyte_hashid = video_p100._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_cost_per_thruplay                         cost_per_thruplay   ON ads.airbyte_hashid = cost_per_thruplay._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_video_continuous_2_sec_watched_actions    unique_video        ON ads.airbyte_hashid = unique_video._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_unique_actions                            unique_actions      ON ads.airbyte_hashid = unique_actions._airbyte_facebook_ads_insights_age_and_gender_hashid
JOIN    bettrads-develop.{{ var('BQ_DATASET')}}.facebook_ads_insights_age_and_gender_video_p100_watched_actions                video               ON ads.airbyte_hashid = video._airbyte_facebook_ads_insights_age_and_gender_hashid
GROUP BY airbyte_hashid
)
SELECT  ads.day
        ,ads.account_id 
        ,ads.account_name
        ,ads.campaign_id 
        ,ads.campaign_name
        ,ads.adset_id 
        ,ads.adset_name
        ,ads.ad_id 
        ,ads.ad_name
        ,ads.age  
        ,ads.gender  
        ,ads.date_start
        ,ads.date_stop
        ,ads.optimization_goal
        ,ads.objective
        ,ads.buying_type
        ,ads.spend                                                                                                      as investment
        ,ads.impressions
        ,ads.frequency
        ,ads.reach
        ,IFNULL(ROUND(cpm,2), 0)                                                                                        as cpm
        ,IFNULL(ROUND(unested.cpi,2), 0)                                                                                as cpi    
        ,IFNULL(ROUND(unested.cpc,2), 0)                                                                                as cpc  
        ,IFNULL(ROUND(inline_link_click_ctr,2), 0)                                                                      as ctr
        ,IFNULL(ROUND(unested.cpv,2), 0)                                                                                as cpv  
        ,IFNULL(ROUND(SAFE_DIVIDE(spend,conversions),2),0)                                                              as cpa
        ,IFNULL(ROUND(unested.cpe,2), 0)                                                                                as cpe
        ,IFNULL(unested.link_click, 0)                                                                                  as clicks 
        ,IFNULL(SAFE_DIVIDE(unested.conversion_all, unested.conversions_duplicates_count),0)                            as conversion_all
        ,IFNULL(SAFE_DIVIDE(unested.conversions, unested.conversions_duplicates_count), 0) + unested.fb_pixel_lead      as conversions
        ,IFNULL(unested.post_engagement, 0)                                                                             as engagement
        ,IFNULL(unested.comments, 0)                                                                                    as comments
        ,IFNULL(unested.post_reaction, 0)                                                                               as reactions
        ,IFNULL(unested.video_view, 0)                                                                                  as video_view
        ,IFNULL(unested.lead, 0)                                                                                        as lead
        ,IFNULL(unested.omni_app_install, 0)                                                                            as downloads
        ,IFNULL(unested.omni_complete_registration, 0)                                                                  as orders
        ,IFNULL(unested.video_p25, 0)                                                                                   as views_25
        ,IFNULL(unested.video_p50, 0)                                                                                   as views_50
        ,IFNULL(unested.video_p75, 0)                                                                                   as views_75
        ,IFNULL(unested.video_p100, 0)                                                                                  as views_100
        ,IFNULL(unested.landing_page_view, 0)                                                                           as visits
        ,IFNULL(ROUND(cp_visit,2), 0)                                                                                   as cp_visit    
        ,IFNULL(ROUND(cp_lead,2), 0)                                                                                    as cp_lead 
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.post_engagement,impressions), 1000),2), 0)                      as remarketing     
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.post_engagement,reach), 1000),2), 0)                            as engagement_rate 
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.conversions,unested.landing_page_view), 1000),2), 0)            as conversions_rate
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.landing_page_view ,unested.link_click), 1000),2), 0)            as connect_rate
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.video_view ,impressions), 1000),2), 0)                          as vtr
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.video_p50 ,impressions), 1000),2), 0)                           as vtr_50
        ,IFNULL(ROUND(SAFE_MULTIPLY(SAFE_DIVIDE(unested.video_p100,impressions), 1000),2), 0)                           as vtr_100
        ,IFNULL(ROUND(fb_pixel_add_to_wishlist,2),0)                                                                    as fb_pixel_add_to_wishlist
        ,IFNULL(ROUND(fb_pixel_lead,2),0)                                                                               as fb_pixel_lead
        ,IFNULL(ROUND(fb_pixel_complete_registration,2),0)                                                              as fb_pixel_complete_registration
        ,IFNULL(ROUND(fb_pixel_purchase,2),0)                                                                           as fb_pixel_purchase
        ,IFNULL(ROUND(fb_pixel_add_payment_info,2),0)                                                                   as fb_pixel_add_payment_info
        ,IFNULL(ROUND(fb_pixel_add_to_cart,2),0)                                                                        as fb_pixel_add_to_cart
        ,IFNULL(ROUND(fb_pixel_custom,2),0)                                                                             as fb_pixel_custom
        ,IFNULL(ROUND(fb_pixel_initiate_checkout,2),0)                                                                  as fb_pixel_initiate_checkout
        ,IFNULL(ROUND(fb_pixel_search,2),0)                                                                             as fb_pixel_search
        ,IFNULL(ROUND(fb_pixel_view_content,2),0)                                                                       as fb_pixel_view_content
        ,ingestion_datetime_at
     FROM facebook_ads_ads_age_and_gender as ads
LEFT JOIN facebook_ads_open_columns as unested 
ON ads.airbyte_hashid = unested.airbyte_hashid

