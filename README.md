# EventSQL

Events over SQL.

Simple, Reliable, Fast.

Able to publish and consume thousands of events per second on a single Postgres instance.

With sharding, it can easily support tens of thousands events per second for virtually endless scalability.

For scalability details, see [benchmarks](/benchmarks/README.md).

## How it works

We just need to have three tables (postgres syntax):

```sql
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
```

To consume messages, we just need to periodically (every one to a few seconds) do:

```sql
BEGIN;

SELECT * FROM consumer 
WHERE topic = :topic AND name = :c_name 
FOR UPDATE SKIP LOCKED;

SELECT * FROM event
WHERE topic = :topic AND (:last_event_id IS NULL OR id > :last_event_id)
ORDER BY id LIMIT N;

(process events)

UPDATE consumer 
SET last_event_id = :id,
    last_consumption_at = :now 
WHERE topic = :topic AND name = :c_name;
```

Optionally, to increase throughput & concurrency, we might have a partitioned topic and consumers (-1 partition standing
for not partitioned topic/consumer).

Distribution of partitioned events is a sole responsibility of publisher - the library provides sensible default (random
distribution).
Consumption of such events per partition (0 in an example) might look like this:

```sql
BEGIN;

SELECT * FROM consumer 
WHERE topic = :topic AND name = :c_name AND partition = 0 
FOR UPDATE SKIP LOCKED;

SELECT * FROM event
WHERE topic = :topic AND partition = 0 AND (:last_event_id IS NULL OR id > :last_event_id)
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

## How to use it

`EventSQL` is an entrypoint to the whole library. It requires standard Java `javax.sql.DataSource` or a list of
them:

```java

import com.binaryigor.eventsql.EventSQL;
import javax.sql.DataSource;
// dialect of your events backend - POSTGRES, MYSQL, MARIADB and so on;
// as of now, only POSTGRES has fully tested support
import org.jooq.SQLDialect;

var eventSQL = new EventSQL(dataSource, SQLDialect.POSTGRES);
ver shardedEventSQL = new EventSQL(dataSources, SQLDialect.POSTGRES);
```

Sharded version works in the same vain - it just assumes that topics and consumers are hosted on multiple dbs.

### Topics and Consumers

Having `EventSQL` instance, we can register topics and their consumers:

```java
// all operations are idempotent
eventSQL.registry()
  // -1 stands for not partitioned topic  
  .registerTopic(new TopicDefinition("account_created", -1))
  .registerTopic(new TopicDefinition("invoice_issued", 5))
  // thirds argument (true/false) determines whether Consumer is partitioned or not      
  .registerConsumer(new ConsumerDefinition("account_created", "consumer-1", false))
  .registerConsumer(new ConsumerDefinition("invoice_issued", "consumer-2", true));
```

Topics and consumers can be both partitioned and not partitioned (-1 stands for not partitioned).
**Partitioned topics allow to have partitioned consumers, increasing parallelism.**
Parallelism of partitioned consumers is as high as consumed topic number of partitions - events have ordering guarantee within a partition.
As a consequence, for a given consumer, each partition can be processed only by a single thread at the time.

For a consumer to be partitioned (third argument in the example) its topic must be partitioned as well - it will have the same number of partitions.
The opposite does not have to be true - consumer might not be partitioned but related topic can;
it has performance implications though, since as described above, consumer parallelism is capped at its number of partitions.

**For sharding, partitions are multiplied by the number of shards.**

For example, if we have *3 shards and a topic with 10 partitions - each shard will host 10 partitions, giving 30 partitions in total*.
Same with consumers of a sharded topic - they will be all multiplied by the number of shards.

For events, it works differently - in the example above, *each shard will host ~ 33% (1/3) of the topic events data*.

There will be *30 consumer instances* in this particular case - `3 shards * 10 partitions`; each consuming from one partition hosted on a given shard.
Each event will be published to a one partition of a single shard - events are unique globally, across all shards.

### Publishing

We can publish single events and batches of arbitrary data and type:
```java
var publisher = eventSQL.publisher();

// with - 1 argument, if topic is partitioned, it will be published to a random partition
publisher.publish(new EventPublication("txt_topic", -1, "txt event".getBytes(StandardCharsets.UTF_8)));
publisher.publish(new EventPublication("raw_topic", 1, new byte[]{1, 2, 3}));
publisher.publish(new EventPublication("json_topic", 2,
  """
  {
    "id": 2,
    "name: "some-user"
  }
  """.getBytes(StandardCharsets.UTF_8)));

// events can have keys and metadata as well
publisher.publish(new EventPublication("txt_topic", -1,
  "event-key",
  "txt event".getBytes(StandardCharsets.UTF_8),
  Map.of("some-tag", "some-meta-info")));


// events can be published in batches, for improved throughput
publisher.publishAll(List.of(
  new EventPublication("txt_topic", -1, "txt event 1".getBytes(StandardCharsets.UTF_8)),
  new EventPublication("txt_topic", -1, "txt event 2".getBytes(StandardCharsets.UTF_8)),
  new EventPublication("txt_topic", -1, "txt event 3".getBytes(StandardCharsets.UTF_8))));
```

### Consuming

We can have both single event and batch consumers:
```java
var consumers = eventSQL.consumers();

consumers.startConsumer("txt_topic", "single-consumer", (Event e) -> {
  // handle single event
});
// with more frequent polling - by default it is 1 second
consumers.startConsumer("txt_topic", "single-consumer-customized", (Event e) -> {
  // handle single event
}, Duration.ofMillis(100));

consumers.startBatchConsumer("txt_topic", "batch-consumer", (List<Event> events) -> {
  // handle events batch
}, // customize batch behavior:
  // minEvents, maxEvents,
  // pollingDelay and maxPollingDelay - how long to wait for minEvents
  EventSQLConsumers.ConsumptionConfig.of(5, 100,
    Duration.ofSeconds(1), Duration.ofSeconds(10)));
```

### Dead Letter Topics (DLT)

If we register a topic with the DLT as follows:
```java
eventSQL.registry()
  .registerTopic(new TopicDefinition("account_created", -1))
  .registerTopic(new TopicDefinition("account_created_dlt", -1));
```
Under certain circumstances, it will have special treatment.

When a consumer throws `EventSQLConsumptionException`, `DefaultDLTEventFactory` takes it over and publishes failed event to the associated dlt if it can find one:
```java
...

@Override
public Optional<EventPublication> create(EventSQLConsumptionException exception, String consumer) {
  var event = exception.event();

  var dltTopic = event.topic() + "_dlt";
  var dltTopicDefinitionOpt = topicDefinitionsCache.getLoadingIf(dltTopic, true);
  if (dltTopicDefinitionOpt.isEmpty()) {
    return Optional.empty();
  }
    
  ...

  // creates dlt event    
```

This factory can be customized by using another `EventSQL` constructor or by calling `EventSQLConsumers.configureDLTEventFactory` method.

What is also worth noting is that any exception thrown by single event consumer is wrapped into `EventSQLConsumptionException` automatically - see *ConsumerWrapper.class*.

When you use `consumers.startBatchConsumer` you have to do wrapping yourself.


## How to get it

Maven:
```
TODO: publish it
```
Gradle:
```
TODO: publish it
```