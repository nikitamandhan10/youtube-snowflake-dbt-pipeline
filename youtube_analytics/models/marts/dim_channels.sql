{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

channel_metrics as (
    select
        channel_title,
        count(distinct video_id) as total_videos,
        count(distinct trending_date) as total_trending_days,
        count(distinct country_code) as countries_reached,
        count(distinct category_id) as categories_used,
        sum(views) as total_views,
        avg(views) as avg_views,
        max(views) as max_views,
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
        avg(days_to_trend) as avg_days_to_trend,
        min(days_to_trend) as min_days_to_trend,
        avg(title_length) as avg_title_length,
        avg(description_length) as avg_description_length,
        avg(tag_count) as avg_tag_count,
        avg(case when comments_disabled then 1 else 0 end) as pct_comments_disabled,
        avg(case when ratings_disabled then 1 else 0 end) as pct_ratings_disabled,
        min(trending_date) as first_trending_date,
        max(trending_date) as last_trending_date,
        mode(category_name) as primary_category
    from enriched_videos
    group by channel_title
),

channel_dimension as (
    select
        {{ dbt_utils.generate_surrogate_key(['channel_title']) }} as channel_key,
        channel_title,
        total_videos,
        total_trending_days,
        countries_reached,
        categories_used,
        total_views,
        avg_views,
        max_views,
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
        avg_days_to_trend,
        min_days_to_trend,
        avg_title_length,
        avg_description_length,
        avg_tag_count,
        pct_comments_disabled,
        pct_ratings_disabled,
        first_trending_date,
        last_trending_date,
        datediff(day, first_trending_date, last_trending_date) as trending_span_days,
        primary_category,
        case
            when avg_views >= 5000000 then 'Top Tier'
            when avg_views >= 1000000 then 'High Performer'
            when avg_views >= 100000 then 'Mid Performer'
            else 'Growing'
        end as channel_tier,
        current_timestamp() as dbt_updated_at
    from channel_metrics
)

select * from channel_dimension