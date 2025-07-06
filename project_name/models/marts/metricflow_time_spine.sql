{{ config(materialized = 'table') }}

with days as (
    {{
        dbt_utils.date_spine(
            datepart='day',
            start_date="cast('2000-01-01' as date)",
            end_date="cast('2027-01-01' as date)"
        )
    }}
),

final as (
    select cast(date_day as date) as date_day
    from days
)

select * from final