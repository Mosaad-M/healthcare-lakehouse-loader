-- Providers, SCD type 2: one row per version of a provider's directory entry.
-- The directory arrives as full snapshots and Silver keeps only the latest, so versions come from
-- the Bronze batches. A version starts when a provider first appears, when its attributes change,
-- or when it returns after dropping out; it ends when the batch that changes or drops it lands.
-- valid_from is when that batch landed (when we learned of the change), not when it happened at
-- the source. A provider's first version is backdated to 1900-01-01 so earlier claims find it.
--
-- Incremental: the table keeps its own history. Each run reads the open versions it already has and
-- only the Bronze batches after its watermark (the last batch that changed anything), and writes
-- only the versions that start or end (materialization iceberg_upsert). A batch is never needed again
-- once applied, and a key never changes once written. The result equals a build over every batch at
-- once. Batches must reach Bronze in landing order, which the ingest pipeline guarantees (each pass
-- takes batches in landing order and stops at the first failure).
-- An empty table, or one written before first_seen_at existed, is built from every batch in Bronze.
-- A file with no rows leaves no batch in Bronze, so it ends no versions.
{{ config(materialized='iceberg_upsert', unique_key='provider_sk') }}

with prior as (
    {% if var('gold_deploy', false) -%}
    -- At deploy the table may not exist yet: an empty prior with the table's columns.
    select cast(null as varchar) as provider_sk, cast(null as varchar) as npi,
        cast(null as varchar) as provider_name, cast(null as varchar) as specialty,
        cast(null as varchar) as state, cast(null as boolean) as active,
        cast(null as timestamptz) as valid_from, cast(null as timestamptz) as valid_to,
        cast(null as boolean) as is_current, cast(null as bigint) as version,
        cast(null as timestamptz) as first_seen_at, cast(null as varchar) as source_batch_id,
        cast(null as varchar) as ended_by_batch_id, cast(null as varchar) as partner
    where false
    {%- else -%}
    select * from {{ this }}
    {%- endif %}
),

-- Rows written before first_seen_at existed carry no watermark: rebuild them all (same keys).
kept as (
    select * from prior where not exists (select 1 from prior where first_seen_at is null)
),

-- The last batch applied that changed something: every such batch started or ended a version.
watermark as (
    select mark_at, mark_batch_id from (
        select first_seen_at as mark_at, source_batch_id as mark_batch_id from kept
        union all
        select valid_to, ended_by_batch_id from kept where valid_to is not null)
    order by mark_at desc, mark_batch_id desc
    limit 1
),

-- Scalar subqueries, not a join: DuckDB turns the lower bound on _ingested_at (Bronze's partition
-- source) into a file filter, so a run reads only the files at or after the watermark.
new_rows as (
    select b.* from {{ source('bronze', 'provider_directory__providers') }} b
    where b._ingested_at >= coalesce((select mark_at from watermark), timestamptz '1900-01-01 00:00:00+00')
        and ((select mark_at from watermark) is null
            or b._ingested_at > (select mark_at from watermark)
            or b._batch_id > (select mark_batch_id from watermark))
),

ordered as (
    select _batch_id, landed_at, row_number() over (order by landed_at, _batch_id) as batch_seq
    from (select _batch_id, min(_ingested_at) as landed_at from new_rows group by _batch_id)
),

-- Batch 0 is the state as of the watermark (the open versions); batches 1.. are the new ones.
-- Batches after the watermark that changed nothing are read again, and change nothing again.
observed as (
    select npi, provider_name, specialty, state, active, 0 as batch_seq, provider_sk as prior_sk
    from kept where valid_to is null
    union all
    select r.npi, r.provider_name, r.specialty, r.state, r.active, o.batch_seq, null
    from new_rows r join ordered o on o._batch_id = r._batch_id
),

marked as (
    select *,
        coalesce(
            lag(batch_seq) over w <> batch_seq - 1
            or lag(provider_name) over w is distinct from provider_name
            or lag(specialty) over w is distinct from specialty
            or lag(state) over w is distinct from state
            or lag(active) over w is distinct from active,
            true) as starts_version
    from observed
    window w as (partition by npi order by batch_seq)
),

islands as (
    select npi, island, min(batch_seq) as first_seq, max(batch_seq) as last_seq, max(prior_sk) as prior_sk,
        any_value(provider_name) as provider_name, any_value(specialty) as specialty,
        any_value(state) as state, any_value(active) as active
    from (
        select *, sum(case when starts_version then 1 else 0 end) over (
            partition by npi order by batch_seq rows between unbounded preceding and current row) as island
        from marked)
    group by npi, island
),

-- A provider's versions so far, and whether one is still open (that open one is island 1).
history as (
    select npi, max(version) as versions, bool_or(valid_to is null) as has_open
    from kept group by npi
),

-- Open versions that a new batch ends: the same row, closed.
ended as (
    select k.* replace (n.landed_at as valid_to, false as is_current, n._batch_id as ended_by_batch_id)
    from islands i
    join kept k on k.provider_sk = i.prior_sk
    join ordered n on n.batch_seq = i.last_seq + 1
    where i.first_seq = 0
),

-- Versions that a new batch starts.
started as (
    select i.*, f.landed_at as first_seen_at, f._batch_id as source_batch_id,
        n.landed_at as valid_to, n._batch_id as ended_by_batch_id,
        coalesce(h.versions, 0) + i.island - (case when h.has_open then 1 else 0 end) as version
    from islands i
    join ordered f on f.batch_seq = i.first_seq
    left join ordered n on n.batch_seq = i.last_seq + 1
    left join history h on h.npi = i.npi
    where i.first_seq > 0
),

versions as (
    select npi, provider_name, specialty, state, active,
        case when version = 1 then timestamptz '1900-01-01 00:00:00+00' else first_seen_at end as valid_from,
        valid_to, version, first_seen_at, source_batch_id, ended_by_batch_id
    from started
)

select
    md5(concat(npi, '|', cast(epoch_us(valid_from) as varchar))) as provider_sk,
    npi,
    provider_name,
    specialty,
    state,
    active,
    valid_from,
    valid_to,
    valid_to is null as is_current,
    cast(version as bigint) as version,
    first_seen_at,
    source_batch_id,
    ended_by_batch_id,
    {{ partner_of('bronze', 'provider_directory__providers') }} as partner
from versions
union all by name
select * from ended
