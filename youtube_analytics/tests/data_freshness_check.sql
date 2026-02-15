-- Test for data freshness: ensure data is loaded within expected timeframe
select
    max(trending_date) as latest_date,
    current_date() as today,
    datediff(day, max(trending_date), current_date()) as days_since_last_load
from {{ ref('stg_youtube_videos') }}
having days_since_last_load > 7
