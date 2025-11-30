{% macro apply_seed_constraints(this, model) -%}
{%- for col_name, col in model.columns.items() -%}
  {# Check if the column is marked nullable in information_schema #}
  {%- set check_nullable = "select count(*) as cnt from information_schema.columns where upper(table_catalog) = upper('" ~ this.database ~ "') and upper(table_schema) = upper('" ~ this.schema ~ "') and upper(table_name) = upper('" ~ this.identifier ~ "') and upper(column_name) = upper('" ~ col_name ~ "') and is_nullable = 'YES'" -%}
  {%- set res = run_query(check_nullable) -%}
  {%- set is_nullable = false -%}
  {%- if res and res.columns and res.columns[0].values() -%}
    {%- set is_nullable = (res.columns[0].values()[0] | int) > 0 -%}
  {%- endif -%}

  {%- if is_nullable -%}
    {# only attempt to set NOT NULL if there are no NULL values in the column #}
    {%- set null_count_sql = "select count(*) as cnt from " ~ this ~ " where " ~ adapter.quote(col_name) ~ " is null" -%}
    {%- set null_res = run_query(null_count_sql) -%}
    {%- set null_count = 0 -%}
    {%- if null_res and null_res.columns and null_res.columns[0].values() -%}
      {%- set null_count = null_res.columns[0].values()[0] | int -%}
    {%- endif -%}

    {%- if null_count == 0 -%}
      {%- set alter_sql = "alter table " ~ this ~ " alter column " ~ adapter.quote(col_name) ~ " set not null" -%}
      {% do run_query(alter_sql) %}
    {%- else -%}
      {% do log("Skipping SET NOT NULL for " ~ this ~ "." ~ col_name ~ " because " ~ null_count ~ " NULL values exist", info=True) %}
    {%- endif -%}
  {%- endif -%}

  {# for *_CODE columns add a unique (or PK) constraint only if no such constraint exists on the column #}
  {%- if col_name.endswith('_CODE') -%}
    {%- set constraint_name = (this.identifier ~ '_' ~ col_name ~ '_uk') | upper -%}
    {# Use the provided TABLE_CONSTRAINTS() query to check for an existing UNIQUE constraint #}
    {%- set check_const_sql = "SELECT 1 FROM " ~ this.database ~ ".INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '" ~ this.schema ~ "' AND TABLE_NAME = '" ~ this.identifier ~ "' AND CONSTRAINT_NAME = '" ~ constraint_name ~ "' AND CONSTRAINT_TYPE = 'UNIQUE'" -%}
    {%- set res2 = run_query(check_const_sql) -%}
    {%- set const_exists = false -%}
    {%- if res2 -%}
      {%- set const_exists = true -%}
    {%- endif -%}

    {%- if not const_exists -%}
      {%- set add_const = "alter table " ~ this ~ " add constraint " ~ constraint_name ~ " unique(" ~ adapter.quote(col_name) ~ ")" -%}
      {% do run_query(add_const) %}
    {%- else -%}
      {% do log("Constraint exists for " ~ this ~ "." ~ col_name ~ "; skipping", info=True) %}
    {%- endif -%}
  {%- endif -%}
{%- endfor -%}
{%- endmacro -%}
