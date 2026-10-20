{#
  Upsert into an existing Iceberg table by `unique_key`: the model's SQL returns only the rows that
  are new or changed, and they replace the rows with the same keys, in one transaction. The model
  may read its own table ({{ this }}): its result is staged first (and committed, since a DuckDB
  transaction writes to one database only), so the delete cannot change what the insert writes.
  The table must already exist (created by loader/gold.py with its properties).
#}
{% materialization iceberg_upsert, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set key = config.require('unique_key') -%}
  {%- set staged = 'temp.main.__upsert_' ~ this.identifier -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('stage') -%}
    CREATE OR REPLACE TEMP TABLE {{ staged.split('.')[-1] }} AS {{ sql }};
  {%- endcall %}
  {% do adapter.commit() %}
  {% call statement('main') -%}
    DELETE FROM {{ target_relation }} WHERE {{ key }} IN (SELECT {{ key }} FROM {{ staged }});
    INSERT INTO {{ target_relation }} BY NAME SELECT * FROM {{ staged }};
  {%- endcall %}
  {% do adapter.commit() %}
  {% call statement('cleanup') -%}
    DROP TABLE IF EXISTS {{ staged }};
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
