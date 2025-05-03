CREATE DATABASE events;

CREATE USER events WITH password 'events';
ALTER DATABASE events OWNER TO events;

\c events;
SET ROLE events;

CREATE TABLE topic (
  name TEXT PRIMARY KEY,
  partitions SMALLINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE event (
  topic TEXT NOT NULL,
  id BIGSERIAL NOT NULL,
  partition SMALLINT NOT NULL,
  key TEXT,
  value BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  metadata JSON NOT NULL,
  PRIMARY KEY (topic, id)
) PARTITION BY LIST (topic);

CREATE TABLE consumer (
  topic TEXT NOT NULL,
  name TEXT NOT NULL,
  partition SMALLINT NOT NULL,
  last_event_id BIGINT,
  last_consumption_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (topic, name, partition)
);
