-- One row per day, 2000 through 2049: the semantic layer's time spine (dbt/MetricFlow need one to
-- resolve time dimensions). A Gold table like any other, in the de-identified catalog: it holds
-- dates only, no data from any source.
select cast(range as date) as date_day
from range(date '2000-01-01', date '2050-01-01', interval 1 day)
