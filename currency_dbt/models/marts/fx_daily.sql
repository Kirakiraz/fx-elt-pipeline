
SELECT
    source_date,
    base_currency,
    target_currency,
    rate,
    LAG(rate) OVER w AS prev_rate,
    ROUND((rate - LAG(rate) OVER w) / NULLIF(LAG(rate) OVER w, 0) * 100, 4)
        AS daily_change_pct,
    ROUND(AVG(rate) OVER (w ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 6)
        AS ma_7d,
    ROUND(AVG(rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 6)
        AS ma_30d,
    ROUND(
        STDDEV_SAMP(rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 6
    ) AS volatility_30d
FROM {{ ref('stg_exchange_rate') }}
WINDOW w AS (PARTITION BY base_currency, target_currency ORDER BY source_date)
