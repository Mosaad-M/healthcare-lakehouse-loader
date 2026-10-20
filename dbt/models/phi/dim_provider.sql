-- Providers, SCD type 2: one row per version of a provider's directory entry.
-- The directory arrives as full snapshots and Silver keeps only the latest, so versions come from
-- the Bronze batches. A version starts when a provider first appears, when its attributes change,
-- or when it returns after dropping out; it ends when the batch that changes or drops it lands.
-- valid_from is when that batch landed (when we learned of the change), not when it happened at
-- the source. A provider's first version is backdated to 1900-01-01 so earlier claims find it.
-- A file with no rows leaves no batch in Bronze, so it ends no versions.
-- Rebuilt in full from Bronze on every run: the same batches always give the same rows and keys.
with batches as (
    select _batch_id, min(_ingested_at) as landed_at
    from {{ source('bronze', 'provider_directory__providers') }}
    group by _batch_id
),

ordered as (
    select _batch_id, landed_at, row_number() over (order by landed_at, _batch_id) as batch_seq
    from batches
),

observed as (
    select p.npi, p.provider_name, p.specialty, p.state, p.active, o.batch_seq, o._batch_id
    from {{ source('bronze', 'provider_directory__providers') }} p
    join ordered o on o._batch_id = p._batch_id
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

numbered as (
    select *,
        sum(case when starts_version then 1 else 0 end) over (
            partition by npi order by batch_seq rows between unbounded preceding and current row) as version
    from marked
),

versions as (
    select npi, version, min(batch_seq) as first_seq, max(batch_seq) as last_seq,
        arg_min(_batch_id, batch_seq) as first_batch_id,
        any_value(provider_name) as provider_name, any_value(specialty) as specialty,
        any_value(state) as state, any_value(active) as active
    from numbered
    group by npi, version
),

dated as (
    select v.*,
        case when v.version = 1 then timestamptz '1900-01-01 00:00:00+00' else f.landed_at end as valid_from,
        n.landed_at as valid_to  -- the next batch after the version's last one; null while current
    from versions v
    join ordered f on f.batch_seq = v.first_seq
    left join ordered n on n.batch_seq = v.last_seq + 1
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
    first_batch_id as source_batch_id,
    {{ partner_of('bronze', 'provider_directory__providers') }} as partner
from dated
