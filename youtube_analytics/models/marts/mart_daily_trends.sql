{{
    config(
        materialized='table',
        tags=['marts', 'analytics']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

daily_aggregates as (
    select
        trending_date,
        country_code,
        
        -- Video counts
        count(distinct video_id) as videos_trending,
        count(distinct channel_title) as unique_channels,
        count(distinct category_id) as unique_categories,
        
        -- View metrics
        sum(views) as total_views,
        avg(views) as avg_views,
        max(views) as max_views,
        min(views) as min_views,
        
        -- Engagement metrics
        sum(likes) as total_likes,
        sum(dislikes) as total_dislikes,
        sum(comment_count) as total_comments,
        avg(engagement_rate) as avg_engagement_rate,
        avg(like_ratio) as avg_like_ratio,
        
        -- Distribution of video performance
        sum(case when view_category = 'Viral (10M+)' then 1 else 0 end) as viral_videos,
        sum(case when view_category = 'Very High (5M-10M)' then 1 else 0 end) as very_high_videos,
        sum(case when view_category = 'High (1M-5M)' then 1 else 0 end) as high_videos,
        sum(case when view_category = 'Medium (100K-1M)' then 1 else 0 end) as medium_videos,
        sum(case when view_category = 'Low (<100K)' then 1 else 0 end) as low_videos,
        
        -- Sentiment distribution
        avg(case when sentiment_category = 'Excellent' then 1 else 0 end) as pct_excellent_sentiment,
        avg(case when sentiment_category = 'Good' then 1 else 0 end) as pct_good_sentiment,
        avg(case when sentiment_category = 'Average' then 1 else 0 end) as pct_average_sentiment,
        avg(case when sentiment_category = 'Poor' then 1 else 0 end) as pct_poor_sentiment,
        
        -- Content characteristics
        avg(title_length) as avg_title_length,
        avg(description_length) as avg_description_length,
        avg(tag_count) as avg_tag_count,
        avg(days_to_trend) as avg_days_to_trend,
        
        -- Disabled features
        avg(case when comments_disabled then 1 else 0 end) as pct_comments_disabled,
        avg(case when ratings_disabled then 1 else 0 end) as pct_ratings_disabled
        
    from enriched_videos
    group by trending_date, country_code
),

with_trends as (
    select
        *,
        
        -- Calculate day-over-day changes
        lag(videos_trending) over (partition by country_code order by trending_date) as prev_day_videos,
        videos_trending - lag(videos_trending) over (partition by country_code order by trending_date) as video_count_change,
        
        lag(avg_views) over (partition by country_code order by trending_date) as prev_day_avg_views,
        avg_views - lag(avg_views) over (partition by country_code order by trending_date) as avg_views_change,
        
        -- Moving averages (7-day)
        avg(videos_trending) over (
            partition by country_code 
            order by trending_date 
            rows between 6 preceding and current row
        ) as videos_7day_ma,
        
        avg(avg_views) over (
            partition by country_code 
            order by trending_date 
            rows between 6 preceding and current row
        ) as views_7day_ma,
        
        -- Day attributes
        dayname(trending_date) as day_name,
        dayofweek(trending_date) as day_of_week,
        week(trending_date) as week_number,
        month(trending_date) as month_number,
        monthname(trending_date) as month_name
        
    from daily_aggregates
)

select 
    {{ dbt_utils.generate_surrogate_key(['trending_date', 'country_code']) }} as daily_trend_key,
    *,
    current_timestamp() as dbt_updated_at
from with_trends