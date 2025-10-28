{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('raw', 'raw_youtube_videos') }}
),

cleaned_data as (
    select
        -- IDs and Keys
        video_id,
        country_code,
        category_id,
        
        -- Dates
        to_date(trending_date, 'YY.DD.MM') as trending_date,
        publish_time as published_at,
        date(publish_time) as publish_date,
        
        -- Video Info
        trim(title) as title,
        trim(channel_title) as channel_title,
        coalesce(trim(description), '') as description,
        coalesce(trim(tags), '') as tags,
        thumbnail_link,
        
        -- Metrics
        coalesce(views, 0) as views,
        coalesce(likes, 0) as likes,
        coalesce(dislikes, 0) as dislikes,
        coalesce(comment_count, 0) as comment_count,
        
        -- Flags
        coalesce(comments_disabled, false) as comments_disabled,
        coalesce(ratings_disabled, false) as ratings_disabled,
        coalesce(video_error_or_removed, false) as video_error_or_removed,
        
        -- Metadata
        load_timestamp,
        
        -- Derived fields
        case 
            when views > 0 then (likes + dislikes) / views::float 
            else 0 
        end as engagement_rate,
        
        case 
            when (likes + dislikes) > 0 then likes / (likes + dislikes)::float 
            else null 
        end as like_ratio,
        
        datediff(day, date(publish_time), to_date(trending_date, 'YY.DD.MM')) as days_to_trend,
        
        case 
            when tags = '[none]' or tags = '' then 0
            else array_size(split(tags, '|'))
        end as tag_count,
        
        length(title) as title_length,
        length(description) as description_length
        
    from source_data
    where video_error_or_removed = false
        and video_id is not null
)

select * from cleaned_data