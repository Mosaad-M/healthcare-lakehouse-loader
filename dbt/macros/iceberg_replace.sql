{#
  Replace an existing Iceberg table's contents in one transaction: DELETE then INSERT,
  committed together. The table must already exist (created by loader/gold.py with its properties);
  Iceberg tables cannot be renamed through DuckDB, so dbt's default table materialization does not
  apply.
#}
{% materialization iceberg_replace, adapter='duckdb' %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    DELETE FROM {{ target_relation }};
    INSERT INTO {{ target_relation }} BY NAME {{ sql }};
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {% do adapter.commit() %}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
