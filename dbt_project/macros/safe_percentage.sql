{% macro safe_percentage(numerator, denominator) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then null
        else ({{ numerator }}::decimal / {{ denominator }}::decimal) * 100
    end
{% endmacro %}
