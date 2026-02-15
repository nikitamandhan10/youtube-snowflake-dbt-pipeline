-- Test for referential integrity between fact and dimension tables
select
    f.channel_title,
    count(*) as orphaned_records
from {{ ref('fct_trending_videos') }} f
left join {{ ref('dim_channels') }} d
    on f.channel_title = d.channel_title
where d.channel_title is null
group by f.channel_title
