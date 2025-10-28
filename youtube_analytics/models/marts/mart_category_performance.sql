{{
    config(
        materialized='table',
        tags=['marts', 'analytics']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

category_analysis as (
    select
        country_code,
        category_name,
        count(distinct video_id) as total_videos,
        count(distinct channel_title) as unique_channels,
        count(distinct trending_date) as days_on_trending,
        sum(views) as total_views,
        avg(views) as avg_views,
        max(views) as max_views,
        percentile_cont(0.5) within group (order by views) as median_views,
        percentile_cont(0.75) within group (order by views) as p75_views,
        percentile_cont(0.90) within group (order by views) as p90_views,
        avg(engagement_rate) as avg_engagement_rate,
        avg(like_ratio) as avg_like_ratio,
        avg(like_rate) as avg_like_rate,
        avg(dislike_rate) as avg_dislike_rate,
        avg(comment_rate) as avg_comment_rate,
        mode(publish_day_name) as most_common_publish_day,
        mode(publish_time_of_day) as most_common_publish_time,
        avg(days_to_trend) as avg_days_to_trend,
        avg(title_length) as avg_title_length,
        avg(description_length) as avg_description_length,
        avg(tag_count) as avg_tag_count,
        sum(case when view_category in ('Viral (10M+)', 'Very High (5M-10M)') then 1 else 0 end) / 
            count(*)::float as viral_rate,
        avg(case when sentiment_category = 'Excellent' then 1 else 0 end) as excellent_sentiment_rate,
        avg(case when comments_disabled then 1 else 0 end) as pct_comments_disabled,
        avg(case when ratings_disabled then 1 else 0 end) as pct_ratings_disabled
    from enriched_videos
    where category_name is not null
    group by country_code, category_name
),

with_rankings as (
    select
        *,
        rank() over (partition by country_code order by total_views desc) as views_rank,
        rank() over (partition by country_code order by avg_engagement_rate desc) as engagement_rank,
        rank() over (partition by country_code order by total_videos desc) as volume_rank,
        (total_views / max(total_views) over (partition by country_code)) * 100 as views_score,
        avg_engagement_rate * 1000 as engagement_score,
        (total_videos / max(total_videos) over (partition by country_code)) * 100 as volume_score
    from category_analysis
)

select
    {{ dbt_utils.generate_surrogate_key(['country_code', 'category_name']) }} as category_perf_key,
    *,
    (views_score * 0.5 + engagement_score * 0.3 + volume_score * 0.2) as overall_performance_score,
    current_timestamp() as dbt_updated_at
from with_rankings