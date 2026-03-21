CREATE TABLE IF NOT EXISTS aircraft_points (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  callsign TEXT NOT NULL,
  observed_at INTEGER NOT NULL,
  lat REAL NOT NULL,
  lon REAL NOT NULL,
  altitude INTEGER,
  groundspeed INTEGER,
  heading REAL,
  squawk TEXT,
  aircraft_code TEXT
);

CREATE INDEX IF NOT EXISTS idx_aircraft_points_callsign_time
ON aircraft_points (callsign, observed_at);

CREATE TABLE IF NOT EXISTS latest_positions (
  callsign TEXT PRIMARY KEY,
  observed_at INTEGER NOT NULL,
  lat REAL NOT NULL,
  lon REAL NOT NULL
);
