# EventSQL

Events over SQL.

## How does it work

We just need to have two tables:
```sql
CREATE TABLE event (
  topic TEXT NOT NULL,
  id BIGSERIAL NOT NULL,
  partition SMALLINT NOT NULL,
  key TEXT,
  value BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  metadata JSONB NOT NULL,
  PRIMARY KEY (topic, id)
) PARTITION BY LIST (topic);

CREATE TABLE consumer (
  topic TEXT NOT NULL,
  name TEXT NOT NULL,
  partition SMALLINT NOT NULL,
  last_event_id BIGINT,
  last_consumption_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (topic, name, partition)
);
```

To consume messages, we just need to periodically (every one to a few seconds) do:
```sql
BEGIN;

SELECT * FROM consumer 
WHERE topic = : topic AND name = :c_name 
FOR UPDATE SKIP LOCKED;

SELECT * FROM event
WHERE (:last_event_id IS NULL) OR id > last_event_id
ORDER BY id LIMIT N;

(process events)

UPDATE consumer 
SET last_event_id = :id,
    last_consumption_at = :now 
WHERE topic = :topic AND name = :c_name;
```

Optionally, to increase throughput & concurrency, we might have partitioned topic and consumers (-1 partition standing for not partitioned topic/consumer).

Distribution of partitioned events is a sole responsibility of publisher - the library provides sensible default (random distribution).
Consumption of such events per partition (0 in example) might look like this:
```sql
BEGIN;

SELECT * FROM consumer 
WHERE topic = :topic AND name = :c_name AND partition = 0 
FOR UPDATE SKIP LOCKED;

SELECT * FROM event
WHERE (:last_event_id IS NULL) OR id > last_event_id AND partition = 0
ORDER BY id LIMIT N;

(process events)

UPDATE consumer 
SET last_event_id = :id,
    last_consumption_at = :now
WHERE topic = :topic AND name = :c_name AND partition = 0;
```

Limitation being that if consumer is partitioned, it must have the exact same number of partition as in the topic
definition.
It's a rather acceptable tradeoff and easy to enforce at the library level.

## TODO

* performance benchmarks and various examples
* usage examples
* built-in outbox
* expiring events/TTL?
* compact topics - unique key
* join, aka streams
* increase code coverage
* JavaDocs


