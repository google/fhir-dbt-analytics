{% macro flatten_column(column) -%}
  {{ return(adapter.dispatch('flatten_column', 'fhir_dbt_analytics') (column)) }}
{%- endmacro %}


{% macro default__flatten_column(column) -%}
  {{ return (column.flatten()) }}
{%- endmacro %}


{% macro spark__flatten_column(column) -%}
  {% set EXPECT_DATA_TYPE = "EXPECT_DATA_TYPE" %}
  {% set EXPECT_COLUMN_NAME = "EXPECT_COLUMN_NAME" %}
  {% set EXPECT_COMMA_OR_GREATER_THAN = "EXPECT_COMMA_OR_GREATER_THAN" %}
  {% set EXPECT_COLON = "EXPECT_COLON" %}

  {% if not "<" in column.data_type %}
    {{ return ([]) }}
  {% endif %}

  {% set tokens = _tokenize(column.data_type) %}
  {% set vars = { "state": EXPECT_DATA_TYPE } %}
  {% set path = [column.name + "."] %}
  {% set flat_columns = [] %}

  {% for token in tokens %}

    {% if vars.state == EXPECT_COMMA_OR_GREATER_THAN %}
      {% if token == "," %}
          {{ vars.update({ "state": EXPECT_COLUMN_NAME }) }}
      {% else %}
          {{ path.pop() }}
      {% endif %}

    {% elif vars.state == EXPECT_COLUMN_NAME %}
      {% if token in KEYWORDS %}
        {{ exceptions.raise_compiler_error("Expected column name, not '" ~ token ~ "'") }}
      {% endif %}
      {{ path.append(token + ".") }}
      {{ vars.update({ "state": EXPECT_COLON }) }}

    {% elif vars.state == EXPECT_COLON %}
      {% if token != ":" %}
        {{ exceptions.raise_compiler_error("Expected colon, not '" ~ token ~ "'") }}
      {%  endif %}
      {{ vars.update({ "state": EXPECT_DATA_TYPE }) }}

    {% elif vars.state == EXPECT_DATA_TYPE %}

      {% if token == "struct<" %}
        {% set data_type = "STRUCT" %}
        {{ vars.update({ "state": EXPECT_COLUMN_NAME }) }}

      {% elif token == "array<" %}
        {% set data_type = "ARRAY" %}
        # Append empty string so that it does not appear in column name
        # but we have something to pop when the array definition ends.
        {{ path.append("") }}
        {{ vars.update({ "state": EXPECT_DATA_TYPE }) }}

      {% else %}
        {% set data_type = token %}
        {{ vars.update({ "state": EXPECT_COMMA_OR_GREATER_THAN }) }}
      {% endif %}

      {% set full_name = ("".join(path))[:-1] %}
      {% if (flat_columns|length > 0
          and flat_columns[-1]["name"] == full_name
          and flat_columns[-1]["data_type"].startswith("ARRAY")) %}
        # Let's replace the data type for the array.
        # TODO(jakuba): For nested arrays, the data type will still have just one ARRAY.
        {{ flat_columns.pop() }}
        {% set data_type = "ARRAY<" + data_type + ">" %}
      {% endif %}

      {{ flat_columns.append({
          "name": full_name,
          "data_type": data_type}) }}

      {% if data_type == token %}
        {{ path.pop() }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ return(flat_columns) }}
{% endmacro %}


{# Returns a list of tokens from `dtype`. #}
{% macro _tokenize(dtype) %}
  {% set KEYWORDS = [
      "struct<",
      "array<",
      ',',
      ':',
      '>']
  %}

  {% set tokens = [] %}
  # In order to keep variables in a `for` cycle, we need to put them in a dictionary.
  {% set vars = { "rest": dtype } %}

  {% for unused in range(0, dtype|length)
     if vars.rest|length > 0 %}
    {% set rest = vars.rest %}
    {{ vars.update({ "token": rest[: find_first_of(rest, ",:>")] }) }}
    {% for keyword in KEYWORDS %}
      {% if rest.startswith(keyword) %}
        {{ vars.update({ "token": keyword }) }}
      {% endif %}
    {% endfor %}
    {{ tokens.append(vars.token) }}
    {{ vars.update({ "rest": rest[vars.token|length:] }) }}
  {% endfor %}

  {{ return (tokens) }}
{% endmacro %}