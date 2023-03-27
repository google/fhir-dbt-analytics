{% macro test_flatten_column() %}

  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col", "data_type": "boolean"}),
    []) }}
  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col", "data_type": "struct<foo:string>"}), [
      {'name': 'col', 'data_type': 'STRUCT'},
      {'name': 'col.foo', 'data_type': 'string'}
    ]) }}
  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col",
      "data_type": "struct<foo:string,col2:struct<bar:int64,baz:string>>"}), [
      {'name': 'col', 'data_type': 'STRUCT'},
      {'name': 'col.foo', 'data_type': 'string'},
      {'name': 'col.col2', 'data_type': 'STRUCT'},
      {'name': 'col.col2.bar', 'data_type': 'int64'},
      {'name': 'col.col2.baz', 'data_type': 'string'},
    ]) }}
  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col",
      "data_type": "struct<col2:struct<bar:date(8),baz:string>>"}), [
        {'name': 'col', 'data_type': 'STRUCT'},
        {'name': 'col.col2', 'data_type': 'STRUCT'},
        {'name': 'col.col2.bar', 'data_type': 'date(8)'},
        {'name': 'col.col2.baz', 'data_type': 'string'},
    ]) }}
  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col",
      "data_type": "struct<col2:struct<ba'r:string,baz:string>,foo:string>"}), [
        {'name': 'col', 'data_type': 'STRUCT'},
        {'name': 'col.col2', 'data_type': 'STRUCT'},
        {'name': 'col.col2.ba\'r', 'data_type': 'string'},
        {'name': 'col.col2.baz', 'data_type': 'string'},
        {'name': 'col.foo', 'data_type': 'string'},
    ]) }}
  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col",
      "data_type": 'struct<foo:string,fruit:array<struct<name:string>>>'}), [
        {'name': 'col', 'data_type': 'STRUCT'},
        {'name': 'col.foo', 'data_type': 'string'},
        {'name': 'col.fruit', 'data_type': 'ARRAY<STRUCT>'},
        {'name': 'col.fruit.name', 'data_type': 'string'},
    ]) }}
  {{ dbt_unittest.assert_equals(
    spark__flatten_column({"name": "col",
      "data_type": 'struct<foo:string,fruit:array<array<struct<name:string>>>>'}), [
        {'name': 'col', 'data_type': 'STRUCT'},
        {'name': 'col.foo', 'data_type': 'string'},
        {'name': 'col.fruit', 'data_type': 'ARRAY<STRUCT>'},
        {'name': 'col.fruit.name', 'data_type': 'string'},
    ]) }}

{% endmacro %}
