{{
    config(
        materialized='incremental',
        unique_key='channel_key',
        on_schema_change='append_new_columns',
        tags=['marts', 'dimension', 'scd2']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
    {% if is_incremental() %}
    where trending_date > (select max(valid_to) from {{ this }} where is_current = true)
    {% endif %}
),

current_channel_state as (
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
        mode(category_name) as primary_category,
        case
            when avg(views) >= 5000000 then 'Top Tier'
            when avg(views) >= 1000000 then 'High Performer'
            when avg(views) >= 100000 then 'Mid Performer'
            else 'Growing'
        end as channel_tier
    from enriched_videos
    group by channel_title
),

{% if is_incremental() %}
existing_records as (
    select * from {{ this }}
    where is_current = true
),

changed_records as (
    select
        e.channel_key,
        e.channel_title,
        e.valid_from,
        current_date() as valid_to,
        false as is_current
    from existing_records e
    inner join current_channel_state c
        on e.channel_title = c.channel_title
    where e.channel_tier != c.channel_tier
        or abs(e.avg_views - c.avg_views) / nullif(e.avg_views, 0) > 0.2
),
{% endif %}

new_records as (
    select
        {{ dbt_utils.generate_surrogate_key(['channel_title', 'current_date()']) }} as channel_key,
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
        channel_tier,
        {% if is_incremental() %}
        coalesce(
            (select valid_from from existing_records e where e.channel_title = current_channel_state.channel_title),
            current_date()
        ) as valid_from,
        {% else %}
        first_trending_date as valid_from,
        {% endif %}
        date('9999-12-31') as valid_to,
        true as is_current,
        current_timestamp() as dbt_updated_at
    from current_channel_state
)

select * from new_records

{% if is_incremental() %}
union all
select * from changed_records
{% endif %}
