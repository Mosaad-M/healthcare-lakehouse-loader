{#
  The partner a source table comes from (its meta.partner in sources.yml), as a SQL string literal.
  Every dimension and fact row carries its partner, so partner offboarding can find and remove it.
#}
{% macro partner_of(source_name, table_name) %}
  {%- if execute -%}
    {%- set node = graph.sources.values() | selectattr('source_name', 'equalto', source_name)
                   | selectattr('name', 'equalto', table_name) | first -%}
    {%- set partner = (node.config.meta or {}).get('partner') if node else none -%}
    {%- if not partner -%}
      {{ exceptions.raise_compiler_error('source ' ~ source_name ~ '.' ~ table_name ~ ' has no meta.partner') }}
    {%- endif -%}
    '{{ partner }}'
  {%- else -%}
    ''
  {%- endif -%}
{% endmacro %}
