-- Claims, one row per claim: the current, corrected version from Silver (partners restate recent
-- service dates, and Silver replaces them). Each claim carries the key of the provider version that
-- was valid on its service date (null if the provider was not in the directory then), so readers
-- join dimensions on equality only. Identifier columns stay out: the member is its subject_id.
with claims as (
    select *, timezone('UTC', cast(service_date as timestamp)) as service_start
    from {{ source('silver', 'acme_claims__claims') }}
)

select
    c.claim_id,
    c.subject_id,
    c.service_date,
    c.diagnosis_code,
    c.procedure_code,
    c.provider_npi,
    p.provider_sk,
    c.billed_amount,
    c.paid_amount,
    {{ partner_of('silver', 'acme_claims__claims') }} as partner,
    c._batch_id as source_batch_id
from claims c
left join {{ ref('dim_provider') }} p
    on p.npi = c.provider_npi
    and p.valid_from <= c.service_start
    and (p.valid_to is null or c.service_start < p.valid_to)
