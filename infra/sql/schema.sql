-- 1) Markets
create table if not exists markets(
  id text primary key,                       -- polymarket market id
  title text not null,
  category text,
  status text check (status in ('open','closed','resolved','paused')) default 'open',
  liquidity_usd numeric,
  token_yes_id text,
  token_no_id text,
  created_at timestamptz,
  updated_at timestamptz,
  raw jsonb                                  -- store full JSON for flexibility
);

-- 2) Trades (append-only)
create table if not exists trades(
  id bigserial primary key,
  market_id text references markets(id),
  ts timestamptz not null,
  price numeric not null,                    -- probability 0..1
  size_usd numeric not null,
  side text check (side in ('buy','sell')),
  maker_wallet text,
  taker_wallet text,
  tx_hash text,
  raw jsonb
);
create index if not exists ix_trades_mkt_ts on trades(market_id, ts);

-- 3) Orderbook snapshots (top-10 depth every N seconds)
create table if not exists ob_snapshots(
  market_id text references markets(id),
  ts timestamptz not null,
  best_bid numeric,
  best_ask numeric,
  bid_depth_usd numeric,
  ask_depth_usd numeric,
  bids jsonb,                                -- [{p,q},...]
  asks jsonb,
  primary key (market_id, ts)
);

-- 4) 1-minute bars (built from trades)
create table if not exists bars_1m(
  market_id text references markets(id),
  ts timestamptz not null,                   -- bar open time
  open numeric, high numeric, low numeric, close numeric,
  volume_usd numeric,
  primary key (market_id, ts)
);
