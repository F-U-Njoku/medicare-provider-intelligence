with date_spine as (
    -- Generates one row for every day between 2008 and 2010
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2008-01-01' as date)",
        end_date="cast('2010-12-31' as date)"
    ) }}
)

select
    date_day as date_actual,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    format_date('%B', date_day) as month_name,
    extract(dayofweek from date_day) as day_of_week,
    case 
        when extract(dayofweek from date_day) in (1, 7) then 'Weekend'
        else 'Weekday'
    end as day_type,
from date_spine