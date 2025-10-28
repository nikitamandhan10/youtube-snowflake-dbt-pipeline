{{
    config(
        materialized='table',
        tags=['marts', 'analytics']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

channel_stats as (
    select
        channel_title,
        country_code,
        count(distinct video_id) as videos_count,
        count(distinct trending_date) as trending_days,
        sum(views) as total_views,
        avg(views) as avg_views,
        max(views) as max_views,
        sum(likes) as total_likes,
        sum(dislikes) as total_dislikes,
        sum(comment_count) as total_comments,
        avg(engagement_rate) as avg_engagement_rate,
        avg(like_ratio) as avg_like_ratio,
        count(distinct category_id) as categories_used,
        mode(category_name) as primary_category,
        sum(case when view_category = 'Viral (10M+)' then 1 else 0 end) as viral_videos,
        avg(days_to_trend) as avg_days_to_trend
    from enriched_videos
    group by channel_title, country_code
),

global_channel_stats as (
    select
        channel_title,
        count(distinct country_code) as countries_reached,
        sum(videos_count) as total_global_videos,
        sum(total_views) as total_global_views,
        avg(avg_views) as avg_global_views,
        avg(avg_engagement_rate) as avg_global_engagement,
        sum(viral_videos) as total_viral_videos
    from channel_stats
    group by channel_title
),

combined as (
    select
        c.*,
        g.countries_reached,
        g.total_global_videos,
        g.total_global_views,
        g.avg_global_views,
        g.avg_global_engagement,
        g.total_viral_videos,
        rank() over (partition by c.country_code order by c.total_views desc) as country_views_rank,
        rank() over (partition by c.country_code order by c.videos_count desc) as country_volume_rank,
        rank() over (partition by c.country_code order by c.avg_engagement_rate desc) as country_engagement_rank,
        rank() over (order by g.total_global_views desc) as global_views_rank,
        rank() over (order by g.total_global_videos desc) as global_volume_rank,
        rank() over (order by g.countries_reached desc) as global_reach_rank,
        case
            when c.total_views >= 100000000 then 'Mega'
            when c.total_views >= 50000000 then 'Large'
            when c.total_views >= 10000000 then 'Medium'
            when c.total_views >= 1000000 then 'Small'
            else 'Micro'
        end as channel_size_tier,
        c.trending_days::float / g.total_global_videos as consistency_score
    from channel_stats c
    left join global_channel_stats g
        on c.channel_title = g.channel_title
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_title', 'country_code']) }} as channel_ranking_key,
    *,
    current_timestamp() as dbt_updated_at
from combined


