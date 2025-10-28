{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

category_metrics as (
    select
        category_id,
        category_name,
        country_code,
        count(distinct video_id) as total_videos,
        count(distinct channel_title) as unique_channels,
        count(distinct trending_date) as total_trending_days,
        sum(views) as total_views,
        avg(views) as avg_views,
        max(views) as max_views,
        percentile_cont(0.5) within group (order by views) as median_views,
        sum(likes) as total_likes,
        avg(likes) as avg_likes,
        sum(dislikes) as total_dislikes,
        avg(dislikes) as avg_dislikes,
        sum(comment_count) as total_comments,
        avg(comment_count) as avg_comments,
        avg(engagement_rate) as avg_engagement_rate,
        avg(like_ratio) as avg_like_ratio,
        avg(like_rate) as avg_like_rate,
        avg(comment_rate) as avg_comment_rate,
        avg(title_length) as avg_title_length,
        avg(description_length) as avg_description_length,
        avg(tag_count) as avg_tag_count,
        avg(days_to_trend) as avg_days_to_trend,
        avg(case when comments_disabled then 1 else 0 end) as pct_comments_disabled,
        avg(case when ratings_disabled then 1 else 0 end) as pct_ratings_disabled
    from enriched_videos
    where category_name is not null
    group by category_id, category_name, country_code
),

category_dimension as (
    select
        {{ dbt_utils.generate_surrogate_key(['category_id', 'country_code']) }} as category_key,
        category_id,
        category_name,
        country_code,
        total_videos,
        unique_channels,
        total_trending_days,
        total_views,
        avg_views,
        max_views,
        median_views,
        total_likes,
        avg_likes,
        total_dislikes,
        avg_dislikes,
        total_comments,
        avg_comments,
        avg_engagement_rate,
        avg_like_ratio,
        avg_like_rate,
        avg_comment_rate,
        avg_title_length,
        avg_description_length,
        avg_tag_count,
        avg_days_to_trend,
        pct_comments_disabled,
        pct_ratings_disabled,
        case
            when avg_views >= 5000000 then 'High Performing'
            when avg_views >= 1000000 then 'Medium Performing'
            else 'Standard'
        end as category_performance_tier,
        current_timestamp() as dbt_updated_at
    from category_metrics
)

select * from category_dimension