-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
