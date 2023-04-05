{% macro assert_string_equals(x, y) %}
  {% set vars = {'pointer': ''} %}
  {% for c in x %}
    {% set pos = loop.index - 1 %}
    {% if y[pos] != c %}
      {{ exceptions.raise_compiler_error(
        "strings differ at character "~pos~" '"~c~"'<>'"~y[pos]~"'\n"
        ~x.replace("\n", "\\n")~" <>\n"~y.replace("\n", "\\n")~"\n"~vars.pointer~"^") }}
    {% endif %}
    # Add two spaces for \n, one space otherwise.
    {{ vars.update({ 'pointer': vars.pointer ~ ('  ' if c == '\n' else ' ') }) }}
  {% endfor %}
{% endmacro %}

