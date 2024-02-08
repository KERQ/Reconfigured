/*
dbt reconfigured macros before this is available as dbt package
*/
{%- macro q(col) -%}
{{ adapter.quote(col) }}
{%- endmacro -%}

{%- macro t(type_) -%}
{{ api.Column.translate_type(type_) }}
{%- endmacro -%}

{% macro case_when() %}
{{ return(adapter.dispatch('case_when')(varargs)) }}
{% endmacro %}

{% macro default__case_when(cases) %}
CASE
{% for case in cases %}{{ case }}{% endfor %}
END
{% endmacro %}

{% macro if_then(when, then) %}
{{ return(adapter.dispatch('if_then')(when, then)) }}
{%- endmacro %}

{% macro default__if_then(when, then) %}
  WHEN {{ when }}
  THEN {{ then }}
{% endmacro %}

{% macro else_(then) %}
{{ return(adapter.dispatch('else_')(then)) }}
{%- endmacro %}

{% macro default__else_(then) %}
  ELSE {{ then }}
{% endmacro %}

{%- macro if_incr(stmt, fallback) -%}
{%- if is_incremental() -%}{{ stmt }}{%-else %}{{ fallback }}{% endif -%}
{% endmacro %}

{%- macro min_ts() -%}
CAST('0001-01-01T00:00:00' AS TIMESTAMP)
{%- endmacro -%}

{%- macro max_ts() -%}
CAST('9999-12-31T23:59:59' AS TIMESTAMP)
{%- endmacro -%}

{%- macro run_start_ts() -%}
CAST({{ string_literal(run_started_at.isoformat()) }} AS {{ t('timestamp') }})
{%- endmacro -%}

{% macro extract(dpart, col) -%}
{{ return(adapter.dispatch('extract')(dpart, col)) }}
{%- endmacro %}

{% macro default__extract(dpart, col) -%}
{{ exceptions.raise_compiler_error("Unsupported target database") }}
{%- endmacro %}

{% macro bigquery__extract(dpart, col) -%}
EXTRACT({{ dpart }} FROM {{ col }})
{%- endmacro %}

{% macro most_recent(col, recency_col) -%}
{{ return(adapter.dispatch('most_recent')(col, recency_col)) }}
{%- endmacro %}

{% macro default__most_recent(col, recency_col) -%}
{{ exceptions.raise_compiler_error("Unsupported target database") }}
{%- endmacro %}

{% macro bigquery__most_recent(col, recency_col) -%}
ANY_VALUE({{ col }} HAVING MAX {{ recency_col }})
{%- endmacro %}

{% macro snowflake__most_recent(col, recency_col) -%}
MAX_BY({{ col }}, {{ recency_col }})
{%- endmacro %}

{% macro redshift__most_recent(col, recency_col) -%}
SPLIT_PART(LISTAGG({{ col }}::varchar, ', ') WITHIN GROUP (ORDER BY {{ recency_col }} DESC), ', ', 1)
{%- endmacro %}

{% macro array_get(val, i) -%}
{{ return(adapter.dispatch('array_get')(val, i)) }}
{% endmacro %}

{% macro default__array_get(val, i) -%}
exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}

{% macro snowflake__array_get(val, i) -%}
GET({{ val }}, {{ i }})
{% endmacro %}

{% macro bigquery__array_get(val, i) -%}
{{ val }}[SAFE_OFFSET({{ i }})]
{% endmacro %}

{% macro redshift__array_get(val, i) -%}
{{ val }}[{{ i }}]
{% endmacro %}

--
{%- macro hash_from_cols(columns, alias=False) -%}
{{ return(adapter.dispatch('hash_from_cols')(columns, alias)) }}
{% endmacro %}
{%- macro default__hash_from_cols(columns, alias) -%}
exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}

{%- macro bigquery__hash_from_cols(columns, alias) -%}
TO_HEX(SHA256(TO_JSON_STRING(JSON_OBJECT(
{%- for column in columns -%}
  '{{ column }}',{%- if alias %} {{ alias }}.{%- else %} {% endif %}{{ column }}
  {%- if not loop.last -%}, {% endif -%}
{%- endfor -%}
))))
{% endmacro %}

{%- macro snowflake__hash_from_cols(columns, alias) -%}
SHA2(TO_JSON(OBJECT_CONSTRUCT(
{%- for column in columns -%}
  '{{ column }}',{%- if alias %} {{ alias }}.{%- else %} {% endif %}{{ column }}
  {%- if not loop.last -%}, {% endif -%}
{%- endfor -%}
)), 256)
{% endmacro %}

{%- macro redshift__hash_from_cols(columns, alias) -%}
SHA2(
{%- for column in columns -%}
  '{{ column }}' || '|' || CAST({%- if alias %}{{ alias }}.{% endif %}{{ column }} AS TEXT)
  {%- if not loop.last -%} || '||' || {% endif -%}
{%- endfor -%}
, 256)
{% endmacro %}

{% macro json_value(val, json_path) %}
  {{ return(adapter.dispatch('json_value')(val, json_path)) }}
{% endmacro %}
{% macro default__json_value() %}
  exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}
{% macro bigquery__json_value(d, json_path) %}
JSON_VALUE({{ d }}, '{{ json_path }}')
{% endmacro %}
{% macro snowflake__json_value(d, json_path) %}
GET_PATH({{ d }}, '{{ json_path | replace('$.', '') }}')
{% endmacro %}
{% macro postgres__json_value(d, json_path) %}
{% set jpath = json_path.replace('$.', '').replace('[', '.').replace(']', '').split('.') %}
{{ d }}
{%- for p in jpath -%}
    {% if loop.last %}->>{% else %}->{% endif -%}
    {% if p.isnumeric() %}{{ p }}{% else %}'{{ p }}'{% endif %}
{%- endfor %}
{% endmacro %}
{% macro redshift__json_value(d, json_path) %}
{% set jpath = json_path.replace('$', '') %}
{{ d }}{{ jpath }}
{% endmacro %}

{% macro md5(d) %}
  {{ return(adapter.dispatch('md5') (d)) }}
{% endmacro %}
{% macro default__md5() %}
  exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}
{% macro redshift__md5(d) %}
MD5({{ d }})
{% endmacro%}
{% macro snowflake__md5(d) %}
MD5({{ d }})
{% endmacro%}
{% macro bigquery__md5(d) %}
TO_HEX(MD5({{ d }}))
{% endmacro %}

{% macro count_distinct(d) %}
  {{ return(adapter.dispatch('count_distinct') (d)) }}
{% endmacro %}
{% macro default__count_distinct(d) %}
COUNT(DISTINCT {{ d }})
{% endmacro %}

{% macro distinct(d) %}
  {{ return(adapter.dispatch('distinct') (d)) }}
{% endmacro %}
{% macro default__distinct(d) %}
DISTINCT {{ d }}
{% endmacro %}

{% macro in(v, arr) %}
  {{ return(adapter.dispatch('in')(v, arr)) }}
{% endmacro %}
{% macro default__in() %}
  exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}
{% macro snowflake__in(v, arr) %}
ARRAY_CONTAINS({{ v }}::variant, {{ arr }})
{% endmacro%}
{% macro bigquery__in(v, arr) %}
{{ v }} IN UNNEST({{ arr }})
{% endmacro %}

{% macro epoch_to_timestamp(d) %}
{{ return(adapter.dispatch('epoch_to_timestamp')(d)) }}
{% endmacro %}
{% macro default__epoch_to_timestamp(d) %}
exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}
{% macro redshift__epoch_to_timestamp(d) %}
TIMESTAMP 'epoch' + {{ d }} * INTERVAL '1 second'
{% endmacro %}

/*Parsers*/
{% macro normalize_email(text) %}
  {{ return(adapter.dispatch('normalize_email') (text)) }}
{% endmacro %}
{% macro default__normalize_email() %}
  exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}
{% macro snowflake__normalize_email(text) %}
  CASE
    WHEN REGEXP_LIKE(
      TRIM({{ text }}),
      '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9\\-.]+$'
    ) THEN REGEXP_REPLACE(LOWER(TRIM({{ text }})), '\\+[\\d\\D]*\\@', '@')
    ELSE NULL
  END
{% endmacro%}
{% macro bigquery__normalize_email(text) %}
  CASE
    WHEN REGEXP_CONTAINS(
      TRIM({{ text }}),
      r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    ) THEN REGEXP_REPLACE(LOWER(TRIM({{ text }})), r'\+[\d\D]*\@', '@')
    ELSE NULL
  END
{% endmacro %}

{% macro parse_website_domain(text) %}
{{ return(adapter.dispatch('parse_website_domain') (text)) }}
{% endmacro %}

{% macro default__parse_website_domain() %}
exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}

{% macro snowflake__parse_website_domain(text) %}
TRIM(LOWER(REGEXP_SUBSTR(
  CAST({{ text }} AS STRING),
  '((http(s)?):\\/\\/)?(www\\.)?([a-zA-Z0-9@:%._\\-\\+~#=]{2,256}\\.[a-z]{2,6})\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)',
  1, 1, 'i', 5
)))
{% endmacro %}
{% macro bigquery__parse_website_domain(text) %}
TRIM(LOWER(REGEXP_EXTRACT(
  CAST({{ text }} AS STRING),
  r'(?:https?:\/\/)?(?:www\.)?([a-zA-Z0-9@:%._\-\+~#=]{2,256}\.[a-z]{2,6})\b[-a-zA-Z0-9@:%_\+.~#?&//=]*'
)))
{% endmacro %}

{% macro redshift__parse_website_domain(text) %}
TRIM(LOWER(REGEXP_REPLACE(
  {{ text }}::TEXT,
  '((http(s)?):\\/\\/)?(www\\.)?([a-zA-Z0-9@:%._\\-\\+~#=]{2,256}\\.[a-z]{2,6})\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)',
  '$5'
)))
{% endmacro %}
/* END RCMACRO */
/* RCMACRO
every_value
*/
{% macro every_value(input) %}
  {{ return(adapter.dispatch('every_value')(input)) }}
{% endmacro %}

{% macro default__every_value(input) %}
  exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}

{% macro bigquery__every_value(input) %}
array_agg({{ input }} ignore nulls)
{% endmacro %}

{% macro snowflake__every_value(input) %}
array_compact(array_agg({{ input }}))
{% endmacro%}
/* END RCMACRO */
/* RCMACRO
every_unique
*/
{% macro every_unique(input) %}
  {{ return(adapter.dispatch('every_unique') (input)) }}
{% endmacro %}

{% macro default__every_unique(input) %}
  exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}

{% macro bigquery__every_unique(input) %}
array_agg(distinct {{ input }} ignore nulls)
{% endmacro %}

{% macro snowflake__every_unique(input) %}
array_compact(array_agg(distinct {{ input }}))
{% endmacro%}
/* END RCMACRO */

{% macro parse_email_domain(text) %}
{{ return(adapter.dispatch('parse_email_domain') (text)) }}
{% endmacro %}

{% macro default__parse_email_domain() %}
exceptions.raise_compiler_error("Unsupported target database")
{% endmacro %}

{% macro snowflake__parse_email_domain(text) %}
TRIM(LOWER(REGEXP_SUBSTR(
  CAST({{ text }} AS STRING),
  '.*@([-a-zA-Z0-9\.]*)',
  1, 1, 'i', 1
)))
{% endmacro %}
{% macro bigquery__parse_email_domain(text) %}
TRIM(LOWER(REGEXP_EXTRACT(
  CAST({{ text }} AS STRING),
  r'.*@([-a-zA-Z0-9\.]*)'
)))
{% endmacro %}

{% macro redshift__parse_email_domain(text) %}
TRIM(LOWER(REGEXP_REPLACE(
  {{ text }}::TEXT,
  '.*@([-a-zA-Z0-9\.]*)',
  '$1'
)))
{% endmacro %}
