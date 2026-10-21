-- Lab results from every lab source, one row per result: metro_lab's lab_results (a collection
-- instant) and acme's mailed results (a collection date only, taken as midnight UTC, and marked so).
-- Each source's result_id is unique only within it, so the key combines source and result_id.
-- value_std and unit_std are the value in the test's standard unit (macro lab_units); null when the
-- unit is not one we know how to convert. subject_id is per source: one person in two labs is two
-- subjects here. Identifier columns stay out.
with results as (
    select result_id, subject_id, collected_at, 'instant' as collected_precision, test_code, value, unit,
        _batch_id, 'lab_results' as source_name, {{ partner_of('silver', 'lab_results__results') }} as partner
    from {{ source('silver', 'lab_results__results') }}
    union all
    select result_id, subject_id, timezone('UTC', cast(collected_on as timestamp)), 'day', test_code, value, unit,
        _batch_id, 'acme_lab_mail', {{ partner_of('silver', 'acme_lab_mail__results') }}
    from {{ source('silver', 'acme_lab_mail__results') }}
),

units as ({{ lab_units() }})

select
    md5(concat(r.source_name, '|', r.result_id)) as lab_result_key,
    r.result_id,
    r.source_name,
    r.partner,
    r.subject_id,
    r.collected_at,
    cast(timezone('UTC', r.collected_at) as date) as collected_date,
    r.collected_precision,
    r.test_code,
    r.value,
    r.unit,
    cast(r.value * u.factor + u.value_offset as double) as value_std,
    u.unit_std,
    r._batch_id as source_batch_id
from results r
left join units u on u.test_code = r.test_code and u.unit_key = lower(trim(r.unit))
