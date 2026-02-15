{{
    config(
        materialized='incremental',
        unique_key='metric_date',
        tags=['monitoring', 'data_quality']
    )
}}

with video_metrics as (
    select
        trending_date as metric_date,
        count(*) as total_records,
        count(distinct video_id) as unique_videos,
        count(distinct channel_title) as unique_channels,
        count(distinct country_code) as unique_countries,
        sum(case when views = 0 then 1 else 0 end) as zero_view_count,
        sum(case when likes = 0 and dislikes = 0 then 1 else 0 end) as no_engagement_count,
        sum(case when comments_disabled then 1 else 0 end) as comments_disabled_count,
        avg(views) as avg_views,
        stddev(views) as stddev_views,
        min(views) as min_views,
        max(views) as max_views,
        percentile_cont(0.5) within group (order by views) as median_views,
        avg(engagement_rate) as avg_engagement_rate,
        count(case when days_to_trend < 0 then 1 end) as negative_days_to_trend_count,
        current_timestamp() as calculated_at
    from {{ ref('int_videos_enriched') }}
    {% if is_incremental() %}
    where trending_date > (select max(metric_date) from {{ this }})
    {% endif %}
    group by trending_date
),

quality_flags as (
    select
        *,
        case when zero_view_count > total_records * 0.05 then true else false end as high_zero_views_flag,
        case when no_engagement_count > total_records * 0.1 then true else false end as high_no_engagement_flag,
        case when negative_days_to_trend_count > 0 then true else false end as data_integrity_issue,
        case 
            when total_records < (lag(total_records) over (order by metric_date)) * 0.5 
            then true else false 
        end as volume_drop_flag
    from video_metrics
)

select * from quality_flags
