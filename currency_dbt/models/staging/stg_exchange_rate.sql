
WITH parsed AS (
    SELECT
        CAST(JSON_VALUE(elem, '$.date') AS DATE) AS source_date,
        JSON_VALUE(elem, '$.base') AS base_currency,
        JSON_VALUE(elem, '$.quote') AS target_currency,
        CAST(JSON_VALUE(elem, '$.rate') AS FLOAT64) AS rate,
        r.fetched_at
    FROM {{ source('raw', 'raw_api_response') }} AS r
    CROSS JOIN UNNEST(JSON_QUERY_ARRAY(r.payload)) AS elem
    WHERE JSON_VALUE(elem, '$.rate') IS NOT NULL
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY source_date, base_currency, target_currency
            ORDER BY fetched_at DESC
        ) AS rn
    FROM parsed
)
SELECT source_date, base_currency, target_currency, rate
FROM ranked
WHERE rn = 1