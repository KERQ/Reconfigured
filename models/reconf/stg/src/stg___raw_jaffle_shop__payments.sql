/*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(da400d16456990db64577be476ccff2f)
*/

{{
  config(
    unique_key = q("___hash"),
    merge_update_columns = [q("___hash")],
    on_schema_change = "append_new_columns",
    full_refresh = var('rc_src_refresh', False)
  )
}}
-- Loading incremental helpers for stg___raw_jaffle_shop__payments
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
             "payment_date",
             "payment_id",
             "payment_description"
           ], "rc1qlinix") +
           " AS " + q("___hash") }},
        -- Column: ___as_of
        {{ "CAST(" +
               "rc1qlinix.payment_date" +
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
               run_start_ts() +
               " AS " + t("timestamp") +
             ")" +
           " AS " + q("___source_loaded_at") }},
        -- Column: ___is_deleted
        {{ "FALSE" +
           " AS " + q("___is_deleted") }},
        -- Column: ___source_id
        {{ "CAST(" + "rc1qlinix.payment_id" + " AS " + t("string") + ")" +
           " AS " + q("___source_id") }},
        -- Column: payment_date
        {{ "rc1qlinix.payment_date" +
           " AS " + q("payment_date") }},
        -- Column: payment_id
        {{ "rc1qlinix.payment_id" +
           " AS " + q("payment_id") }},
        -- Column: payment_description
        {{ "rc1qlinix.payment_description" +
           " AS " + q("payment_description") }}
      FROM {{ source("raw_jaffle_shop", "payments") }} AS rc1qlinix
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
        , {{ q("payment_date") }}
        , {{ q("payment_id") }}
        , {{ q("payment_description") }}
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
      , {{ q("payment_date") }}
      , {{ q("payment_id") }}
      , {{ q("payment_description") }}
    FROM ___rn_added
    WHERE {{ q("___rn") }} = 1
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base

