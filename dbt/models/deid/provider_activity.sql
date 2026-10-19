-- Activity per provider and month, with the provider directory. Small groups are suppressed.
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
from {{ source('silver', 'acme_claims__claims') }} c
join {{ source('silver', 'provider_directory__providers') }} p on p.npi = c.provider_npi
group by all
having count(distinct c.subject_id) >= {{ var('min_cell_size') }}
