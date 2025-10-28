{{
    config(
        materialized='view'
    )
}}

with videos as (
    select * from {{ ref('stg_youtube_videos') }}
),

categories as (
    select * from {{ ref('stg_categories') }}
),

enriched as (
    select
        v.video_id,
        v.country_code,
        v.category_id,
        c.category_name,
        v.trending_date,
        v.published_at,
        v.publish_date,
        v.title,
        v.channel_title,
        v.description,
        v.tags,
        v.thumbnail_link,
        v.title_length,
        v.description_length,
        v.tag_count,
        v.days_to_trend,
        v.views,
        v.likes,
        v.dislikes,
        v.comment_count,
        v.engagement_rate,
        v.like_ratio,
        v.comments_disabled,
        v.ratings_disabled,
        case 
            when v.views > 0 then v.likes / v.views::float 
            else 0 
        end as like_rate,
        case 
            when v.views > 0 then v.dislikes / v.views::float 
            else 0 
        end as dislike_rate,
        case 
            when v.views > 0 then v.comment_count / v.views::float 
            else 0 
        end as comment_rate,
        dayofweek(v.publish_date) as publish_day_of_week,
        dayname(v.publish_date) as publish_day_name,
        hour(v.published_at) as publish_hour,
        case 
            when hour(v.published_at) between 6 and 11 then 'Morning'
            when hour(v.published_at) between 12 and 17 then 'Afternoon'
            when hour(v.published_at) between 18 and 23 then 'Evening'
            else 'Night'
        end as publish_time_of_day,
        case
            when v.views >= 10000000 then 'Viral (10M+)'
            when v.views >= 5000000 then 'Very High (5M-10M)'
            when v.views >= 1000000 then 'High (1M-5M)'
            when v.views >= 100000 then 'Medium (100K-1M)'
            else 'Low (<100K)'
        end as view_category,
        case
            when v.like_ratio >= 0.95 then 'Excellent'
            when v.like_ratio >= 0.85 then 'Good'
            when v.like_ratio >= 0.70 then 'Average'
            else 'Poor'
        end as sentiment_category,
        v.load_timestamp
    from videos v
    left join categories c
        on v.category_id = c.category_id
)
select * from enriched