# EventSQL

Events over SQL.

## How does it work

Having basic event structure like this:
```sql
CREATE TABLE {topic}_event (
  id BIGSERIAL PRIMARY KEY,
  key TEXT,
  value BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL
);
```

We can have kafka-like design:
```sql
CREATE TABLE ${topic}_consumer (
  id TEXT NOT NULL,
  partition SMALLINT NOT NULL DEFAULT -1,
  last_event_id BIGINT,
  last_consumption_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL,
  PRIMARY KEY (id, partition)
);

Then, simply periodically do:
BEGIN;

SELECT * FROM {topic}_consumer WHERE id = :c_id FOR UPDATE SKIP LOCKED;

SELECT * FROM {topic}_event
WHERE (:last_event_id IS NULL) OR id > last_event_id
ORDER BY id LIMIT N;

(process events)

UPDATE {topic}_consumer 
SET last_event_id = :id,
    last_consumption_at = :now 
WHERE id = :s_id;
```

Optionally, we might allow a topic to be partitioned with the following changes:
```sql
-- created by partitioned topic for every event --
CREATE TABLE {topic}_event_partition (
  id BIGINT NOT NULL REFERENCES {topic}_event (id) ON DELETE CASCADE,
  partition SMALLINT NOT NULL,
  PRIMARY_KEY (id, partition);
);

-- then, consumers have an option to consume unpartitioned topic or subscribe to each parition separately --
-- if partitioned, there must be the same number of partitioned consumers as there are partitions --
```

Distribution of partitioned events is a sole responsibility of the producer. 
Consumption of such events per partition (0 in example) might look like this:
```sql
BEGIN;

SELECT * FROM {topic}_consumer WHERE id = :s_id AND partition = 0 FOR UPDATE SKIP LOCKED;

SELECT m.* FROM {topic}_event_partition p
INNER JOIN {topic}_event m ON p.id = m.id AND p.partition = 0
WHERE (:last_event_id IS NULL) OR id > last_event_id
ORDER BY id LIMIT N;

(process events)

UPDATE ${topic}_consumer 
SET last_event_id = :id,
    last_consumption_at = :now
WHERE id = :s_id;
```

Limitation being that if consumer is partitioned, it must have the exact same number of partition as in the topic definition. 
It's a rather acceptable tradeoff and easy to enforce at the library level.

Let's go over some examples.

### user_created topic

Partitioned or not:
```sql
CREATE TABLE user_created_event (
  id BIGSERIAL PRIMARY KEY,
  key TEXT,
  value BYTEA NOT NULL,
  created_at TIMESTAMP NOT NULL
);

CREATE TABLE user_created_event_partition (
  id BIGINT NOT NULL REFERENCES user_created_event(id) ON DELETE CASCADE,
  partition SMALLINT NOT NULL,
  PRIMARY_KEY (id, partition);
);
```

We then might have both partitioned and not partitioned consumers:
```sql
user_created_consumer ('monolithic-app', -1, 33, '2025-04-20T10:16:33.915Z');
user_created_consumer ('micro-app', 0, 22, '2025-04-20T10:16:33.915Z');
user_created_consumer ('micro-app', 1, 77, '2025-04-20T10:16:33.915Z');
```


## TODO

* dlt mechanism (allow to roll some events to dlt_topic or sth similar)
* expiring events? TTL?
* compact topics
* join, aka streams!


