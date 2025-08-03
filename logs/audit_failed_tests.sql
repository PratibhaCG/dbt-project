{% set failure_tables = get_test_failure_tables(target.schema) %}

{% if failure_tables | length == 0 %}
    SELECT NULL AS test_name, NULL AS *
{% else %}
    {% for tname in failure_tables %}
        SELECT '{{ tname }}' AS test_table, *
        FROM {{ target.schema }}.{{ tname }}
        {% if not loop.last %}
            UNION ALL
        {% endif %}
    {% endfor %}
{% endif %}
