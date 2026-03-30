create table if not exists investors (
  filer_cik text primary key,
  name text not null,
  style_tags text[] default '{}'
);

create table if not exists fund_holdings (
  filer_cik text not null references investors(filer_cik) on delete cascade,
  company_id bigint not null references companies(id) on delete cascade,
  period_end date not null,
  shares numeric(24,6),
  value_usd numeric(24,2),
  primary key (filer_cik, company_id, period_end)
);

-- handy map: CIK<->ticker (populate as you ingest)
create table if not exists cik_ticker (
  cik text not null,
  ticker text not null,
  primary key (cik, ticker)
);

create index if not exists idx_holdings_company_period on fund_holdings(company_id, period_end);