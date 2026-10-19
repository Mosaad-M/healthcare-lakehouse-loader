-- One row per member (subject): a PHI mart for care teams. Identifiers stay encrypted:
-- member_name is ciphertext for the subject, readable only through the audited decrypt path.
select
    subject_id,
    max_by(member_name, service_date) as member_name,
    cast(count(*) as bigint) as claims,
    min(service_date) as first_service_date,
    max(service_date) as last_service_date,
    cast(sum(billed_amount) as decimal(18, 2)) as billed_amount,
    cast(sum(paid_amount) as decimal(18, 2)) as paid_amount
from {{ source('silver', 'acme_claims__claims') }}
group by subject_id
