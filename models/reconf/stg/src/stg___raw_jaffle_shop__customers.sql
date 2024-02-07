/*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(17ee75edacdc29b1653b55d5dd4e1bb9)
*/

{{
  config(
    unique_key = q("___hash"),
    merge_update_columns = [q("___hash")],
    on_schema_change = "append_new_columns",
    full_refresh = var('rc_src_refresh', False)
  )
}}
-- Loading incremental helpers for stg___raw_jaffle_shop__customers
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
             "full_name",
             "number_of_orders",
             "customer_id"
           ], "rciulbiv") +
           " AS " + q("___hash") }},
        -- Column: ___as_of
        {{ "RCERR8976" +
           " AS " + q("___as_of") }},
        -- Column: ___loaded_at
        {{ "CAST(" +
               run_start_ts() +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___loaded_at") }},
        -- Column: ___source_loaded_at
        {{ "CAST(" +
               run_start_ts() +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___source_loaded_at") }},
        -- Column: ___is_deleted
        {{ "FALSE" +
           " AS " + q("___is_deleted") }},
        -- Column: ___source_id
        {{ "CAST(" + "rciulbiv.customer_id" + " AS " + t("string") + ")" +
           " AS " + q("___source_id") }},
        -- Column: full_name
        {{ "rciulbiv.full_name" +
           " AS " + q("full_name") }},
        -- Column: number_of_orders
        {{ "rciulbiv.number_of_orders" +
           " AS " + q("number_of_orders") }},
        -- Column: customer_id
        {{ "rciulbiv.customer_id" +
           " AS " + q("customer_id") }}
      FROM {{ source("raw_jaffle_shop", "customers") }} AS rciulbiv
      WHERE {{
        "( " + if_incr("( " + run_start_ts() + " > " + "CAST(" +
            string_literal(max_loaded_at) +
            " AS " + t("timestamp") +
          ")" + " )", "1 = 1") +
        " AND " + "( " + run_start_ts() + " <= " + run_start_ts() + " )" + " )"
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
        , {{ q("full_name") }}
        , {{ q("number_of_orders") }}
        , {{ q("customer_id") }}
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
      , {{ q("full_name") }}
      , {{ q("number_of_orders") }}
      , {{ q("customer_id") }}
    FROM ___rn_added
    WHERE {{ q("___rn") }} = 1
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base

