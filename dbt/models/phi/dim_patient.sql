-- Clinic patients, SCD type 2: one row per version of a patient's tracked attributes (birth year,
-- 3-digit ZIP, plan), from the clinic's change feed (acme_clinic_patients, CDC) in Bronze.
-- A change feed is not a snapshot: a patient missing from a batch is unchanged, and only a delete
-- ends a patient's last version. Within a batch only a patient's latest change counts (cdc_seq
-- orders changes). Across batches the rule is Silver's, applied change by change: a change is ignored
-- if the patient is present and it is not newer than the patient's current row (a redelivered or
-- out-of-order change); a delete leaves nothing to compare with, so this table always agrees with
-- Silver, and gives the same result however its batches are split into runs.
-- valid_from is when the batch that carried the change landed, as in dim_provider. The clinic's own
-- updated_at is kept as source_updated_at but not used: its application sets it, and an update that
-- does not touch it leaves an older time. A patient's first version is backdated to 1900-01-01.
-- Identifier columns (MRN, name, email) are not here: Silver holds their latest ciphertext, by
-- subject_id, for the audited decrypt path. Balance is a measure, not an attribute: not tracked.
--
-- Incremental, as dim_provider: each run reads the open versions it has and only the Bronze batches
-- after its watermark and before its horizon (loader/gold.py history_horizons), and writes only
-- the versions that start, end, or (open ones) have applied a newer change. Keys never change.
{{ config(materialized='iceberg_upsert', unique_key='patient_sk') }}

with recursive prior as (
    {% if var('gold_deploy', false) -%}
    -- At deploy the table may not exist yet: an empty prior with the table's columns.
    select cast(null as varchar) as patient_sk, cast(null as bigint) as patient_id,
        cast(null as varchar) as subject_id, cast(null as integer) as birth_year,
        cast(null as varchar) as zip3, cast(null as varchar) as plan_code,
        cast(null as timestamptz) as valid_from, cast(null as timestamptz) as valid_to,
        cast(null as boolean) as is_current, cast(null as bigint) as version,
        cast(null as boolean) as ended_by_delete, cast(null as decimal(38, 0)) as start_cdc_seq,
        cast(null as decimal(38, 0)) as last_cdc_seq, cast(null as timestamptz) as source_updated_at,
        cast(null as timestamptz) as first_seen_at, cast(null as varchar) as source_batch_id,
        cast(null as varchar) as ended_by_batch_id, cast(null as varchar) as partner
    where false
    {%- else -%}
    select * from {{ this }}
    {%- endif %}
),

kept as (
    select * from prior where not exists (select 1 from prior where first_seen_at is null)
),

-- The last batch applied that started or ended a version.
watermark as (
    select mark_at, mark_batch_id from (
        select first_seen_at as mark_at, source_batch_id as mark_batch_id from kept
        union all
        select valid_to, ended_by_batch_id from kept where valid_to is not null)
    order by mark_at desc, mark_batch_id desc
    limit 1
),

{%- set horizon = var('history_horizon', {}).get('acme_clinic_patients') %}
new_rows as (
    select b.* from {{ source('bronze', 'acme_clinic_patients__patients') }} b
    where b._ingested_at >= coalesce((select mark_at from watermark), timestamptz '1900-01-01 00:00:00+00')
        and ((select mark_at from watermark) is null
            or b._ingested_at > (select mark_at from watermark)
            or b._batch_id > (select mark_batch_id from watermark))
        {%- if horizon %}
        and b._ingested_at < timestamptz '{{ horizon }}'
        {%- endif %}
),

ordered as (
    select _batch_id, landed_at, row_number() over (order by landed_at, _batch_id) as batch_seq
    from (select _batch_id, min(_ingested_at) as landed_at from new_rows group by _batch_id)
),

-- Each patient's latest change in each new batch, numbered per patient in batch order.
latest as (
    select r.*, o.batch_seq, row_number() over (partition by r.patient_id order by o.batch_seq) as k
    from (select *, row_number() over (partition by _batch_id, patient_id order by cdc_seq desc) as rn
          from new_rows) r
    join ordered o on o._batch_id = r._batch_id
    where r.rn = 1
),

-- Silver's rule, change by change: start from each patient's current row (an open version) or none,
-- and take each change unless the patient is present and the change is not newer than its row.
walk as (
    select patient_id, 0 as k, true as is_open, last_cdc_seq as last_seq, cast(null as boolean) as taken
    from kept where valid_to is null
    union all
    select distinct l.patient_id, 0, false, cast(null as decimal(38, 0)), cast(null as boolean)
    from latest l
    where not exists (select 1 from kept k where k.patient_id = l.patient_id and k.valid_to is null)
    union all
    select w.patient_id, l.k,
        case when w.is_open and l.cdc_seq <= w.last_seq then true else l.cdc_op <> 'D' end,
        case when w.is_open and l.cdc_seq <= w.last_seq then w.last_seq
             when l.cdc_op = 'D' then null else l.cdc_seq end,
        not (w.is_open and l.cdc_seq <= w.last_seq)
    from walk w
    join latest l on l.patient_id = w.patient_id and l.k = w.k + 1
),

fresh as (
    select l.* from latest l join walk w on w.patient_id = l.patient_id and w.k = l.k where w.taken
),

-- Batch 0 is the state as of the watermark (the open versions); batches 1.. are the new changes.
observed as (
    select patient_id, subject_id, birth_year, zip3, plan_code, true as present, 0 as batch_seq,
        last_cdc_seq as cdc_seq, cast(null as timestamptz) as updated_at, patient_sk as prior_sk
    from kept where valid_to is null
    union all
    select patient_id, subject_id, birth_year, zip3, plan_code, cdc_op <> 'D', batch_seq, cdc_seq, updated_at, null
    from fresh
),

marked as (
    select *,
        present and coalesce(
            not lag(present) over w
            or lag(birth_year) over w is distinct from birth_year
            or lag(zip3) over w is distinct from zip3
            or lag(plan_code) over w is distinct from plan_code,
            true) as starts_version,
        lead(batch_seq) over w as next_seq,
        lead(present) over w as next_present
    from observed
    window w as (partition by patient_id order by batch_seq)
),

numbered as (
    select *, sum(case when starts_version then 1 else 0 end) over (
        partition by patient_id order by batch_seq rows between unbounded preceding and current row) as island
    from marked
),

-- One row per version touched by this run: where it starts, its last change, and what ends it.
islands as (
    select patient_id, island,
        min(batch_seq) as first_seq,
        max(batch_seq) as last_seq,
        max(prior_sk) as prior_sk,
        max(cdc_seq) as last_cdc_seq,
        arg_min(cdc_seq, batch_seq) as start_cdc_seq,
        arg_min_null(updated_at, batch_seq) as source_updated_at,
        arg_max_null(next_seq, batch_seq) as end_seq,  -- the _null forms keep a NULL "nothing after it"
        arg_max_null(next_present, batch_seq) as end_present,
        any_value(subject_id) as subject_id, any_value(birth_year) as birth_year,
        any_value(zip3) as zip3, any_value(plan_code) as plan_code
    from numbered
    where present
    group by patient_id, island
),

history as (
    select patient_id, max(version) as versions, bool_or(valid_to is null) as has_open
    from kept group by patient_id
),

-- Open versions this run touched: ended (by a change or a delete), or still open with a newer change.
continued as (
    select k.* replace (
        e.landed_at as valid_to,
        e.landed_at is null as is_current,
        case when e.landed_at is null then null else not i.end_present end as ended_by_delete,
        i.last_cdc_seq as last_cdc_seq,
        e._batch_id as ended_by_batch_id)
    from islands i
    join kept k on k.patient_sk = i.prior_sk
    left join ordered e on e.batch_seq = i.end_seq
    where i.first_seq = 0 and (i.end_seq is not null or i.last_cdc_seq > k.last_cdc_seq)
),

started as (
    select i.*, f.landed_at as first_seen_at, f._batch_id as source_batch_id,
        e.landed_at as valid_to, e._batch_id as ended_by_batch_id,
        case when e.landed_at is null then null else not i.end_present end as ended_by_delete,
        coalesce(h.versions, 0) + i.island - (case when h.has_open then 1 else 0 end) as version
    from islands i
    join ordered f on f.batch_seq = i.first_seq
    left join ordered e on e.batch_seq = i.end_seq
    left join history h on h.patient_id = i.patient_id
    where i.first_seq > 0
)

select
    -- Keyed by the batch that started the version: at most one version of a patient starts in a batch
    -- (only its latest change there counts). Not by the change's sequence number: a redelivered insert
    -- after a delete starts a new version with the same sequence number as the first.
    md5(concat(cast(patient_id as varchar), '|', source_batch_id)) as patient_sk,
    patient_id,
    subject_id,
    birth_year,
    zip3,
    plan_code,
    case when version = 1 then timestamptz '1900-01-01 00:00:00+00' else first_seen_at end as valid_from,
    valid_to,
    valid_to is null as is_current,
    cast(version as bigint) as version,
    ended_by_delete,
    start_cdc_seq,
    last_cdc_seq,
    source_updated_at,
    first_seen_at,
    source_batch_id,
    ended_by_batch_id,
    {{ partner_of('bronze', 'acme_clinic_patients__patients') }} as partner
from started
union all by name
select * from continued
