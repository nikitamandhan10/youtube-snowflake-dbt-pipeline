{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select * from {{ source('raw', 'raw_categories') }}
),

flattened_categories as (
    select
        country_code,
        value:id::integer as category_id,
        value:snippet.title::varchar as category_name,
        value:snippet.assignable::boolean as is_assignable,
        load_timestamp
    from source_data,
    lateral flatten(input => category_data:items)
)

select 
    country_code,
    category_id,
    trim(category_name) as category_name,
    coalesce(is_assignable, true) as is_assignable,
    load_timestamp
from flattened_categories
where category_id is not null