-- Activity per provider and month. Name, specialty and state are the provider's as of each claim's
-- service date (dim_provider, SCD type 2); claims whose provider was not in the directory on that
-- date are left out. Small groups are suppressed.
select
    c.provider_npi,
    p.provider_name,
    p.specialty,
    p.state,
    cast(date_trunc('month', c.service_date) as date) as service_month,
    cast(count(*) as bigint) as claims,
    cast(count(distinct c.subject_id) as bigint) as members,
    cast(sum(c.billed_amount) as decimal(18, 2)) as billed_amount,
    cast(sum(c.paid_amount) as decimal(18, 2)) as paid_amount
from {{ ref('fact_claim') }} c
join {{ ref('dim_provider') }} p on p.provider_sk = c.provider_sk
group by all
having count(distinct c.subject_id) >= {{ var('min_cell_size') }}
