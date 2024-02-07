/*
BEGIN OF reconfigured.io
DO NOT EDIT this section
RCSRC(13f94fd22c053350afbf8f7553aa02e6)
*/
WITH
  base AS (
    WITH
      ___myentity___raw_jaffle_shop__orders___hlebsk AS (
        WITH
           ___pre AS (
            SELECT
              -- Column: ___deleted
              {{ "rciunnas." + q("___is_deleted") +
                 " AS " + q("___deleted") }},
              -- Column: ___as_of
              {{ "rciunnas." + q("___as_of") +
                 " AS " + q("___as_of") }},
              -- Column: ___rn
              {{ "ROW_NUMBER()" + " OVER ("
                   + " PARTITION BY " +
                     "rciunnas." + q("order_id") + ", " + "rciunnas." + q("___source_id")
                   + " ORDER BY " +
                     "rciunnas." + q("___as_of") + " DESC "
                  + " )" +
                 " AS " + q("___rn") }},
              -- Column: first_attribute
              {{ "rciunnas." + q("order_id") +
                 " AS " + q("first_attribute") }},
              -- Column: order_id__kcmxrw
              {{ "rciunnas." + q("order_id") +
                 " AS " + q("order_id__kcmxrw") }}
            FROM {{ ref("stg___raw_jaffle_shop__orders") }} AS rciunnas
            WHERE {{
              "( " + "rciunnas." + q("___as_of") + " <= " + "CURRENT_TIMESTAMP()" + " )"
            }}
          )
        SELECT
          -- Column: first_attribute
          {{ "rc5snpfg." + q("first_attribute") +
             " AS " + q("first_attribute") }}
        FROM {{ "___pre" }} AS rc5snpfg
        WHERE {{
          "( " + "( " + "rc5snpfg." + q("___deleted") + " = " + "FALSE" + " )" +
          " AND " + "( " + "rc5snpfg." + q("___rn") + " = " + "CAST(" +
              string_literal("1") +
              " AS " + t("int") +
            ")" + " )" + " )"
        }}
        GROUP BY {{
          "rc5snpfg." + q("first_attribute")
        }}
      )
      , ___id_chain AS (
        SELECT DISTINCT {{ q("first_attribute") }} AS {{ q("___entity_id") }} FROM (
          SELECT {{ q("first_attribute") }} FROM ___myentity___raw_jaffle_shop__orders___hlebsk
        ) WHERE {{ q("first_attribute") }} IS NOT NULL
      )
    SELECT
      -- Column: first_attribute
      {{ "rca73wyw." + q("___entity_id") +
         " AS " + q("first_attribute") }},
      -- Column: second_attribute
      {{ "RCERR8976" +
         " AS " + q("second_attribute") }}
    
    FROM ___id_chain AS rca73wyw
    LEFT JOIN ___myentity___raw_jaffle_shop__orders___hlebsk AS rcyqqv7u
      ON rca73wyw.{{ q("___entity_id") }} = rcyqqv7u.{{ q("first_attribute") }}
)
/*
feel free to edit what comes after this
END OF reconfigured.io
*/

SELECT *
FROM base

