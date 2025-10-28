select
    video_id,
    trending_date,
    country_code,
    views,
    likes,
    dislikes,
    comment_count
from {{ ref('fct_trending_videos') }}
where views < 0
   or likes < 0
   or dislikes < 0
   or comment_count < 0