-- current_ceos.sql  (non-destructive: does NOT touch tenure_features)
DROP TABLE IF EXISTS current_ceos;

CREATE TABLE current_ceos AS
WITH current_rows AS (
  SELECT
    ct.person_name                                AS CEO,
    COALESCE(NULLIF(cm.company_name,''), NULLIF(ct.company,'')) AS company,
    UPPER(ct.ticker)                              AS ticker,
    COALESCE(NULLIF(ct.role,''),'CEO')            AS role,
    DATE(ct.start_date)                           AS current_tenure_start,
    ROW_NUMBER() OVER (
      PARTITION BY UPPER(ct.ticker)
      ORDER BY DATE(ct.start_date) DESC NULLS LAST
    ) AS rn
  FROM ceo_tenures ct
  LEFT JOIN company_metadata cm ON LOWER(cm.ticker)=LOWER(ct.ticker)
  WHERE ct.end_date IS NULL
)
SELECT CEO, company, ticker, role, current_tenure_start
FROM current_rows
WHERE rn = 1
ORDER BY CEO, company, ticker;