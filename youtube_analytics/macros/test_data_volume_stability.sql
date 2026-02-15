{% macro test_data_volume_stability(model, column_name, date_column, threshold=0.5) %}

with daily_counts as (
    select
        {{ date_column }} as metric_date,
        count(*) as record_count
    from {{ model }}
    group by {{ date_column }}
),

with_previous as (
    select
        metric_date,
        record_count,
        lag(record_count) over (order by metric_date) as previous_count
    from daily_counts
)

select
    metric_date,
    record_count,
    previous_count,
    abs(record_count - previous_count) / nullif(previous_count, 0) as pct_change
from with_previous
where abs(record_count - previous_count) / nullif(previous_count, 0) > {{ threshold }}
    and previous_count is not null

{% endmacro %}
