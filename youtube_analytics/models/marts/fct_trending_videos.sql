{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

with enriched_videos as (
    select * from {{ ref('int_videos_enriched') }}
),

fact_table as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['video_id', 'country_code', 'trending_date']) }} as trending_video_key,
        
        -- Natural keys
        video_id,
        country_code,
        category_id,
        trending_date,
        
        -- Video attributes (for filtering/grouping)
        channel_title,
        category_name,
        title,
        
        -- Dates
        publish_date,
        published_at,
        days_to_trend,
        publish_day_of_week,
        publish_day_name,
        publish_hour,
        publish_time_of_day,
        
        -- Engagement metrics
        views,
        likes,
        dislikes,
        comment_count,
        
        -- Calculated metrics
        engagement_rate,
        like_ratio,
        like_rate,
        dislike_rate,
        comment_rate,
        
        -- Video characteristics
        title_length,
        description_length,
        tag_count,
        
        -- Flags
        comments_disabled,
        ratings_disabled,
        
        -- Categories
        view_category,
        sentiment_category,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from enriched_videos
)

select * from fact_table