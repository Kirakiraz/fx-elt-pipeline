-- ============================================================
-- MART LAYER: Analysis view — derived FX metrics
-- vw_fx_metrics computes prev_rate, daily_change_pct, 7d/30d moving
-- averages, and 30d volatility on top of fact_exchange_rate.
-- Row-based windows (ROWS BETWEEN), matching standard financial-market
-- ============================================================
CREATE OR REPLACE VIEW mart.vw_fx_metrics AS
SELECT
    d.full_date AS source_date,
    f.base_currency,
    f.target_currency,
    f.rate,
    LAG(f.rate) OVER w AS prev_rate,
    ROUND(
        (f.rate - LAG(f.rate) OVER w) / NULLIF(LAG(f.rate) OVER w, 0) * 100, 4
    ) AS daily_change_pct,
    ROUND(
        AVG(f.rate) OVER (w ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 6
    ) AS ma_7d,
    ROUND(
        AVG(f.rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 6
    ) AS ma_30d,
    ROUND(
        STDDEV_SAMP(f.rate) OVER (w ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 6
    ) AS volatility_30d
FROM mart.fact_exchange_rate AS f
INNER JOIN mart.dim_date AS d ON f.date_key = d.date_key
WINDOW w AS (PARTITION BY f.base_currency, f.target_currency ORDER BY f.date_key);
