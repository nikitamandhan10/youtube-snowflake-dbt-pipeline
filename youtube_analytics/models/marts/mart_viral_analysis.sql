{{
    config(
        materialized='table',
        tags=['marts', 'analytics']
    )
}}

-- Analysis of what makes videos go viral
with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

viral_videos as (
    select *
    from enriched_videos
    where view_category in ('Viral (10M+)', 'Very High (5M-10M)')
),

viral_patterns as (
    select
        country_code,
        category_name,
        
        -- Volume
        count(distinct video_id) as viral_video_count,
        count(distinct channel_title) as channels_with_viral,
        
        -- Publishing patterns
        publish_day_name,
        publish_time_of_day,
        count(*) as videos_in_timeframe,
        
        -- Average characteristics of viral videos
        avg(views) as avg_viral_views,
        avg(likes) as avg_viral_likes,
        avg(engagement_rate) as avg_viral_engagement,
        avg(like_ratio) as avg_viral_like_ratio,
        avg(comment_rate) as avg_viral_comment_rate,
        
        -- Content characteristics
        avg(title_length) as avg_title_length,
        avg(description_length) as avg_description_length,
        avg(tag_count) as avg_tag_count,
        avg(days_to_trend) as avg_days_to_trend,
        
        -- Features
        avg(case when comments_disabled then 1 else 0 end) as pct_comments_disabled,
        avg(case when ratings_disabled then 1 else 0 end) as pct_ratings_disabled
        
    from viral_videos
    group by 
        country_code, 
        category_name, 
        publish_day_name, 
        publish_time_of_day
),

ranked_patterns as (
    select
        *,
        
        -- Rank best times to publish for virality
        rank() over (
            partition by country_code, category_name 
            order by videos_in_timeframe desc
        ) as timing_rank,
        
        -- Calculate relative success rate
        videos_in_timeframe::float / 
            sum(videos_in_timeframe) over (partition by country_code, category_name) 
            as timing_success_rate
            
    from viral_patterns
)

select
    {{ dbt_utils.generate_surrogate_key([
        'country_code', 
        'category_name', 
        'publish_day_name', 
        'publish_time_of_day'
    ]) }} as viral_pattern_key,
    *,
    current_timestamp() as dbt_updated_at
    
from ranked_patterns
where videos_in_timeframe >= 3  -- Filter for statistical significance