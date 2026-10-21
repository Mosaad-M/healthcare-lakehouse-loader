{#
  Each lab test's standard unit, and how to convert the units partners send into it:
  value_std = value * factor + offset. Units are matched trimmed and case-insensitively. A result in
  a unit that is not listed has no standard value and is left out of the aggregates, never guessed.
  LOINC codes; factors from the conventional unit definitions (glucose 18.016 mg/dL per mmol/L,
  cholesterol 38.67 mg/dL per mmol/L, HbA1c NGSP % = 0.09148 * IFCC mmol/mol + 2.152).
#}
{% macro lab_units() %}
    select * from (values
        ('2345-7', 'mg/dl',    'mg/dL', 1.0,     0.0),    -- glucose
        ('2345-7', 'mmol/l',   'mg/dL', 18.016,  0.0),
        ('2093-3', 'mg/dl',    'mg/dL', 1.0,     0.0),    -- total cholesterol
        ('2093-3', 'mmol/l',   'mg/dL', 38.67,   0.0),
        ('4548-4', '%',        '%',     1.0,     0.0),    -- HbA1c
        ('4548-4', 'mmol/mol', '%',     0.09148, 2.152),
        ('718-7',  'g/dl',     'g/dL',  1.0,     0.0),    -- hemoglobin
        ('718-7',  'g/l',      'g/dL',  0.1,     0.0)
    ) as u(test_code, unit_key, unit_std, factor, value_offset)
{% endmacro %}
