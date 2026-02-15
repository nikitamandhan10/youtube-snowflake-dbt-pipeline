-- Test for anomalies: detect unusual spikes or drops in metrics
with daily_stats as (
    select
        trending_date,
        count(*) as record_count,
        avg(views) as avg_views
    from {{ ref('fct_trending_videos') }}
    group by trending_date
),

stats_with_moving_avg as (
    select
        trending_date,
        record_count,
        avg_views,
        avg(record_count) over (order by trending_date rows between 6 preceding and current row) as moving_avg_count,
        stddev(record_count) over (order by trending_date rows between 6 preceding and current row) as stddev_count
    from daily_stats
)

select
    trending_date,
    record_count,
    moving_avg_count,
    abs(record_count - moving_avg_count) / nullif(stddev_count, 0) as z_score
from stats_with_moving_avg
where abs(record_count - moving_avg_count) / nullif(stddev_count, 0) > 3
    and stddev_count > 0
