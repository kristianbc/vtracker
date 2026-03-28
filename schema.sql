create table if not exists public.aircraft_points (
  id bigserial primary key,
  callsign text not null,
  observed_at bigint not null,
  lat double precision not null,
  lon double precision not null,
  altitude integer,
  groundspeed integer,
  heading double precision,
  squawk text,
  aircraft_code text
);

create unique index if not exists idx_aircraft_points_unique_snapshot
on public.aircraft_points (callsign, observed_at, lat, lon);

create index if not exists idx_aircraft_points_callsign_time
on public.aircraft_points (callsign, observed_at);

create index if not exists idx_aircraft_points_observed_at
on public.aircraft_points (observed_at);

create table if not exists public.latest_positions (
  callsign text primary key,
  observed_at bigint not null,
  lat double precision not null,
  lon double precision not null
);

create index if not exists idx_latest_positions_observed_at
on public.latest_positions (observed_at);

create table if not exists public.ingested_snapshots (
  observed_at bigint primary key,
  created_at bigint not null
);

create index if not exists idx_ingested_snapshots_created_at
on public.ingested_snapshots (created_at);
