create table if not exists public.aircraft_points (
  id bigserial primary key,
  callsign text not null,
  session_id bigint not null,
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
on public.aircraft_points (callsign, session_id, observed_at, lat, lon);

create index if not exists idx_aircraft_points_callsign_time
on public.aircraft_points (callsign, session_id, observed_at);

create index if not exists idx_aircraft_points_observed_at
on public.aircraft_points (observed_at);

create table if not exists public.latest_positions (
  callsign text primary key,
  session_id bigint not null,
  observed_at bigint not null,
  lat double precision not null,
  lon double precision not null
);

create index if not exists idx_latest_positions_observed_at
on public.latest_positions (observed_at);
