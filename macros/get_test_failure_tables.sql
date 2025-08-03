{% macro get_test_failure_tables(schema_name) %}
    {% set results = run_query(
        "SELECT table_name
         FROM `{{ target.project }}.{{ schema_name }}.INFORMATION_SCHEMA.TABLES`
         WHERE table_schema = '" ~ schema_name ~ "'
           AND table_name LIKE '%_test'"
    ) %}
    {% set all_tables = [] %}
    {% if results is not none %}
      {% for row in results %}
        {% set _ = all_tables.append(row[0]) %}
      {% endfor %}
    {% endif %}
    {{ return(all_tables) }}
{% endmacro %}
