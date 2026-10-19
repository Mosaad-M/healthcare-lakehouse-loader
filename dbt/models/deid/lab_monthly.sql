-- Lab results per test and month. Small groups are suppressed.
select
    test_code,
    unit,
    cast(date_trunc('month', collected_at) as date) as collected_month,
    cast(count(*) as bigint) as results,
    cast(count(distinct subject_id) as bigint) as patients,
    cast(avg(value) as double) as mean_value,
    cast(min(value) as double) as min_value,
    cast(max(value) as double) as max_value
from {{ source('silver', 'lab_results__results') }}
group by all
having count(distinct subject_id) >= {{ var('min_cell_size') }}
