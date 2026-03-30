create extension if not exists timescaledb;
create extension if not exists pgcrypto;

-- core reference tables
create table if not exists companies (
  id bigserial primary key,
  ticker text not null unique,
  cik text,
  name text not null,
  sector text,
  index_memberships text[] default '{}'
);

-- time-series: prices (hypertable)
create table if not exists prices (
  company_id bigint not null references companies(id) on delete cascade,
  ts timestamptz not null,
  open numeric(18,6), high numeric(18,6), low numeric(18,6),
  close numeric(18,6), volume bigint,
  primary key (company_id, ts)
);
select create_hypertable('prices','ts', if_not_exists=>true);

-- time-series: signals
create table if not exists signals (
  company_id bigint not null references companies(id) on delete cascade,
  ts timestamptz not null,
  name text not null,
  value double precision not null,
  meta jsonb default '{}'::jsonb,
  primary key (company_id, ts, name)
);
select create_hypertable('signals','ts', if_not_exists=>true);

-- simple seed
insert into companies(ticker,name,sector) values
  ('MSFT','Microsoft Corporation','Technology')
on conflict do nothing;