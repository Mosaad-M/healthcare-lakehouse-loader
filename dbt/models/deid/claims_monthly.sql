-- Claims by month of service and diagnosis. De-identified: no person-level rows, and groups with
-- fewer than min_cell_size distinct members are suppressed.
select
    cast(date_trunc('month', service_date) as date) as service_month,
    diagnosis_code,
    cast(count(*) as bigint) as claims,
    cast(count(distinct subject_id) as bigint) as members,
    cast(sum(billed_amount) as decimal(18, 2)) as billed_amount,
    cast(sum(paid_amount) as decimal(18, 2)) as paid_amount
from {{ ref('fact_claim') }}
group by all
having count(distinct subject_id) >= {{ var('min_cell_size') }}
