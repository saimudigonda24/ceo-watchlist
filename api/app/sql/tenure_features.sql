-- tenure_features.sql (robust, no self-reference)
DROP TABLE IF EXISTS tenure_features;

CREATE TABLE tenure_features AS
WITH base AS (
  SELECT
    ct.person_id,
    ct.person_name,
    COALESCE(cm.company_name, ct.company) AS company,
    ct.ticker AS ct_ticker,
    ct.role,
    DATE(ct.start_date) AS start_date,
    DATE(ct.end_date)   AS end_date,
    COALESCE(cm.sector, 'Unknown') AS sector
  FROM ceo_tenures ct
  LEFT JOIN company_metadata cm
    ON LOWER(cm.ticker) = LOWER(ct.ticker)
),
px AS (
  SELECT
    pd.ticker,
    DATE(pd.d) AS d,
    pd.adj_close,
    pd.sector_return
  FROM prices_daily pd
),
ret_calc AS (
  SELECT
    p.ticker,
    p.d,
    (p.adj_close / NULLIF(LAG(p.adj_close, 63) OVER (PARTITION BY p.ticker ORDER BY p.d), 0.0) - 1.0)
      AS pre_3m_return,
    (
      (LEAD(p.adj_close, 252) OVER (PARTITION BY p.ticker ORDER BY p.d) / NULLIF(p.adj_close, 0.0) - 1.0)
      - (LEAD(p.sector_return, 252) OVER (PARTITION BY p.ticker ORDER BY p.d) - p.sector_return)
    ) AS post_12m_excess_return
  FROM px p
),
daily AS (
  SELECT
    b.person_id,
    b.person_name,
    b.company,
    b.ct_ticker AS ticker,
    b.role,
    b.start_date,
    b.end_date,
    b.sector,
    r.pre_3m_return,
    r.post_12m_excess_return
  FROM base b
  LEFT JOIN ret_calc r
    ON r.ticker = b.ct_ticker
   AND r.d BETWEEN b.start_date AND COALESCE(b.end_date, DATE('now'))
),
agg AS (
  SELECT
    person_id,
    person_name,
    company,
    ticker,
    role,
    start_date,
    end_date,
    sector,
    -- Advanced features placeholder
    0.0 AS cap_alloc_buyback_rate,
    0.0 AS cap_alloc_dilution_rate,
    0.0 AS insider_buy_score,
    0.0 AS insider_sell_score,
    0.0 AS r_and_d_intensity_delta,
    0.0 AS sgna_efficiency_delta,
    0.0 AS headcount_growth_6m,
    0.0 AS transcripts_action_verb_rate,
    0.0 AS transcripts_focus_operational_rate,
    0.0 AS transcripts_focus_product_rate,
    COALESCE(AVG(pre_3m_return), 0.0) AS pre_3m_return,
    AVG(post_12m_excess_return)       AS post_12m_excess_return
  FROM daily
  GROUP BY
    person_id, person_name, company, ticker, role, start_date, end_date, sector
)
SELECT * FROM agg;

WITH current_rows AS (
  SELECT
    ct.person_name AS CEO,
    COALESCE(NULLIF(cm.company_name,''), NULLIF(ct.company,'')) AS company_raw,
    UPPER(ct.ticker) AS ticker,
    COALESCE(NULLIF(ct.role,''), 'CEO') AS role,
    DATE(ct.start_date) AS current_tenure_start,
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(ct.ticker)
      ORDER BY DATE(ct.start_date) DESC NULLS LAST
    ) AS rn
  FROM ceo_tenures ct
  LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(ct.ticker)
  WHERE ct.end_date IS NULL
)
SELECT CEO, company_raw AS company, ticker, role, current_tenure_start
FROM current_rows
WHERE rn = 1
ORDER BY CEO, company, ticker;