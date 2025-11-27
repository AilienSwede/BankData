{% macro propagate_column_comments() %}
  {# Only run at execution time, not during parsing #}
  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {# Current model node from the manifest graph #}
  {% set model_node = graph.nodes[model.unique_id] %}

  {# All upstream nodes (models/sources/etc) this model depends on #}
  {% set upstream_node_ids = [] %}
  {% for node_id in model_node.depends_on.nodes %}
    {% if node_id.startswith('model.') or node_id.startswith('source.') %}
      {% do upstream_node_ids.append(node_id) %}
    {% endif %}
  {% endfor %}

  {# Only inherit if there is exactly ONE upstream node #}
  {% set upstream_node = none %}
  {% if upstream_node_ids | length == 1 %}
    {% set upstream_node = graph.nodes[upstream_node_ids[0]] %}
  {% endif %}

  {# Loop over all columns defined for this model in YAML #}
  {% for col_name, col in model_node.columns.items() %}
    {% set description = (col.description or '') | trim %}

    {# If current column already has a description, keep it #}
    {% if not description and upstream_node is not none %}
      {# Try to inherit from upstream column with the same name #}
      {% set upstream_col = upstream_node.columns.get(col_name) %}
      {% if upstream_col is not none and upstream_col.description %}
        {% set description = upstream_col.description | trim %}
      {% endif %}
    {% endif %}

    {# If we now have a description, write it as a column comment in Snowflake #}
    {% if description %}
      {% set escaped = description.replace("'", "''") %}

      {% if this.type == 'view' %}
        {% set sql %}
          comment on column {{ this }}.{{ adapter.quote(col_name) }}
          is '{{ escaped }}';
        {% endset %}
      {% else %}
        {% set sql %}
          alter table {{ this }}
          modify column {{ adapter.quote(col_name) }}
          comment '{{ escaped }}';
        {% endset %}
      {% endif %}

      {% do run_query(sql) %}
    {% endif %}
  {% endfor %}

  {{ return('') }}
{% endmacro %}
