{% macro find_first_of(haystack, letters) %}
  {% for i in range(0, haystack|length)
     if haystack[i] in letters %}
    {{ return (i) }}
  {% endfor %}

  {{ return(-1) }}
{% endmacro %}
