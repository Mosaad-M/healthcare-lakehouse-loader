-- Lab results per test and month, in the test's standard unit, from every lab source (fact_lab_result).
-- No minimum or maximum: each is one patient's actual result. sum_value lets averages over several
-- months be weighted by results (sum of sum_value over sum of results).
-- Small groups are suppressed. subject_id is per source, so a person in two labs counts twice in
-- `patients`; suppression therefore asks that a single source alone have min_cell_size patients in
-- the group, which no double count can reach.
with per_source as (
    select test_code, unit_std, date_trunc('month', collected_date) as collected_month, source_name,
        count(distinct subject_id) as source_patients
    from {{ ref('fact_lab_result') }}
    where unit_std is not null
    group by all
),

kept as (
    select test_code, unit_std, collected_month
    from per_source
    group by all
    having max(source_patients) >= {{ var('min_cell_size') }}
)

select
    f.test_code,
    f.unit_std as unit,
    cast(date_trunc('month', f.collected_date) as date) as collected_month,
    cast(count(*) as bigint) as results,
    cast(count(distinct f.subject_id) as bigint) as patients,
    cast(avg(f.value_std) as double) as mean_value,
    cast(sum(f.value_std) as double) as sum_value
from {{ ref('fact_lab_result') }} f
join kept k on k.test_code = f.test_code and k.unit_std = f.unit_std
    and k.collected_month = date_trunc('month', f.collected_date)
group by all
