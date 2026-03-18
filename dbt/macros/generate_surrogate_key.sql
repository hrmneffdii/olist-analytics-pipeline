{% macro generate_surrogate_key(columns) %}
    md5(
        concat_ws(
            '-',
            {% for col in columns %}
                cast({{ col }} as varchar)
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
{% endmacro %}