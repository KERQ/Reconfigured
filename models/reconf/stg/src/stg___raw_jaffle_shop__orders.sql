/*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(73929539d722410215391f2a9dd81474)
*/

{{
  config(
    unique_key = q("___hash"),
    merge_update_columns = [q("___hash")],
    on_schema_change = "append_new_columns",
    full_refresh = var('rc_src_refresh', False)
  )
}}
-- Loading incremental helpers for stg___raw_jaffle_shop__orders
{%- set max_loaded_at = "" -%}
{% if is_incremental() %}
{%- call statement("max_loaded_at_stmt", fetch_result=True) -%}
SELECT CAST(MAX({{ q("___loaded_at") }}) AS {{ t('string') }}) FROM {{ this }}
{%- endcall -%}
{%- set max_loaded_at = load_result("max_loaded_at_stmt")["data"][0][0] -%}
{% endif %}
-- End of incremental helpers
WITH
  base AS (
    WITH ___pre AS (
      SELECT
        -- Column: ___hash
        {{ hash_from_cols([
             
           ], "rc1f8tyux") +
           " AS " + q("___hash") }},
        -- Column: ___as_of
        {{ "CAST(" +
               "rc1f8tyux.order_id" +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___as_of") }},
        -- Column: ___loaded_at
        {{ "CAST(" +
               run_start_ts() +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___loaded_at") }},
        -- Column: ___source_loaded_at
        {{ "CAST(" +
               "rc1f8tyux.order_date" +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___source_loaded_at") }},
        -- Column: ___is_deleted
        {{ "rc1f8tyux.order_date" + " IS NOT NULL" +
           " AS " + q("___is_deleted") }},
        -- Column: ___source_id
        {{ "CAST(" + "rc1f8tyux.order_id" + " AS " + t("string") + ")" +
           " AS " + q("___source_id") }},
        -- Column: order_date
        {{ "rc1f8tyux.order_date" +
           " AS " + q("order_date") }},
        -- Column: order_state
        {{ "rc1f8tyux.order_state" +
           " AS " + q("order_state") }},
        -- Column: order_id
        {{ "rc1f8tyux.order_id" +
           " AS " + q("order_id") }}
      FROM {{ ref("orders") }} AS rc1f8tyux
      WHERE {{
        "( " + if_incr("( " + "rc1f8tyux.order_date" + " > " + "CAST(" +
            string_literal(max_loaded_at) +
            " AS " + t("timestamp") +
          ")" + " )", "1 = 1") +
        " AND " + "( " + "rc1f8tyux.order_date" + " <= " + run_start_ts() + " )" + " )"
      }}
    )
    ,___rn_added AS (
      SELECT
        {{ q("___hash") }}
        , {{ q("___as_of") }}
        , {{ q("___loaded_at") }}
        , {{ q("___source_loaded_at") }}
        , {{ q("___is_deleted") }}
        , {{ q("___source_id") }}
        , {{ q("order_date") }}
        , {{ q("order_state") }}
        , {{ q("order_id") }}
        , ROW_NUMBER() OVER (PARTITION BY {{ q("___hash") }} ORDER BY {{ q("___as_of") }} ASC) AS {{ q("___rn") }}
      FROM ___pre
    )
    SELECT
      {{ q("___hash") }}
      , {{ q("___as_of") }}
      , {{ q("___loaded_at") }}
      , {{ q("___source_loaded_at") }}
      , {{ q("___is_deleted") }}
      , {{ q("___source_id") }}
      , {{ q("order_date") }}
      , {{ q("order_state") }}
      , {{ q("order_id") }}
    FROM ___rn_added
    WHERE {{ q("___rn") }} = 1
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base

