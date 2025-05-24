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
  first_event_id BIGINT,
  last_event_id BIGINT,
  last_consumption_at TIMESTAMP,
  consumed_events BIGINT NOT NULL,
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
  -- eventual consistency of auto increment; there is no guarantee that record of id 2 is visible after id 1 record --
  -- in the implementation, we set insert statements timeout to '250 ms' so it is safe --
  AND created_at < (NOW() - interval '333 ms')
ORDER BY id LIMIT :limit;

(process events)

UPDATE consumer 
SET last_event_id = :id,
    last_consumption_at = :now 
WHERE topic = :topic AND name = :c_name;

COMMIT;
```

Optionally, to increase throughput & concurrency, we might have a partitioned topic and consumers (-1 partition standing
for not partitioned topic/consumer/event).

Distribution of partitioned events is the sole responsibility of the publisher.

Consumption of such events per partition (0 in an example) might look like this:

```sql
BEGIN;

SELECT * FROM consumer 
WHERE topic = :topic AND name = :c_name AND partition = 0 
FOR UPDATE SKIP LOCKED;

SELECT * FROM event
WHERE topic = :topic AND partition = 0 AND (:last_event_id IS NULL OR id > :last_event_id)
  -- eventual consistency of auto increment; there is no guarantee that record of id 2 is visible after id 1 record --
  -- in the implementation, we set insert statements timeout to '250 ms' so it is safe --
  AND created_at < (NOW() - interval '333 ms')
ORDER BY id LIMIT :limit;

(process events)

UPDATE consumer 
SET last_event_id = :id,
    last_consumption_at = :now
WHERE topic = :topic AND name = :c_name AND partition = 0;

COMMIT;
```

Limitation being that if consumer is partitioned, it must have the exact same number of partition as the topic
definition has. It's a rather acceptable tradeoff and easy to enforce at the library level.

## How to use it

`EventSQL` is an entrypoint to the whole library. It requires single data source properties or a list of
them:

```java
import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLDialect;

// EventSQL.Dialect is a dialect of your events backend - POSTGRES, MYSQL, MARIADB and so on;
// as of now, only POSTGRES has fully tested support;
// should also work with others but some things - event table partition management for example - works only with Postgres, for others it must be managed manually
var eventSQL = EventSQL.of(new EventSQL.DataSourceProperties(EventSQLDialect.POSTGRES, "dbUrl", "dbUsername", "dbPassword"));
        ver shardedEventSQL = EventSQL.of(
                List.of(
                        new EventSQL.DataSourceProperties(EventSQLDialect.POSTGRES, "db0Url", "db0Username", "db0Password"),
                        new EventSQL.DataSourceProperties(EventSQLDialect.POSTGRES, "db1Url", "db1Username", "db1Password"),
                        new EventSQL.DataSourceProperties(EventSQLDialect.POSTGRES, "db2Url", "db2Username", "db2Password")));
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

Topics and consumers can be both partitioned and not partitioned.
**Partitioned topics allow to have partitioned consumers, increasing parallelism.**
Parallelism of partitioned consumers is as high as consumed topic number of partitions - events have ordering guarantee within a partition.
As a consequence, for a given consumer, each partition can be processed only by a single thread at the time.

For a consumer to be partitioned its topic must be partitioned as well - it will have the same number of partitions. 
The opposite does not have to be true - consumer might not be partitioned but a related topic can; it has performance implications though, since as described above, consumer parallelism is capped at its number of partitions.

**With sharding, partitions are multiplied by the number of shards (db instances).**

For example, if we have *3 shards (3 dbs) and a topic with 10 partitions - each shard (db) will host 10 partitions, giving 30 partitions in total*.
Same with consumers of a sharded topic - they will be all multiplied by the number of shards.

For events, it works differently - in the example above, *each shard will host ~ 33% (1/3) of the topic events data* (assuming even partition distribution).
To get all events, we must read them from all shards.

There will be *30 consumer instances* in this particular case - `3 shards * 10 partitions`; each consuming from one partition hosted on a given shard.
Each event will be published to a one partition of a single shard - as a consequence, events are unique globally, across all shards.

### Publishing

We can publish single events and batches of arbitrary data and type:
```java
var publisher = eventSQL.publisher();

publisher.publish(new EventPublication("txt_topic", "txt event".getBytes(StandardCharsets.UTF_8)));
publisher.publish(new EventPublication("raw_topic", new byte[]{1, 2, 3}));
publisher.publish(new EventPublication("json_topic",
  """
  {
    "id": 2,
    "name: "some-user"
  }
  """.getBytes(StandardCharsets.UTF_8)));

// events can have keys and metadata as well;
// key determines event distribution - if it's null, partition is randomly assigned      
publisher.publish(new EventPublication("txt_topic",
  "event-key",
  "txt event".getBytes(StandardCharsets.UTF_8),
  Map.of("some-tag", "some-meta-info")));


// events can be also published in batches, for improved throughput
publisher.publishAll(List.of(
  new EventPublication("txt_topic", "txt event 1".getBytes(StandardCharsets.UTF_8)),
  new EventPublication("txt_topic", "txt event 2".getBytes(StandardCharsets.UTF_8)),
  new EventPublication("txt_topic", "txt event 3".getBytes(StandardCharsets.UTF_8))));
```

### Partitioner

Event partition is determined by `EventSQLPublisher.Partitioner`. By default, the following implementation is used:
```java
public class DefaultPartitioner implements EventSQLPublisher.Partitioner {

  private static final Random RANDOM = new Random();

  @Override
  public int partition(EventPublication publication, int topicPartitions) {
    if (topicPartitions == -1) {
      return -1;
    }
    if (publication.key() == null) {
      return RANDOM.nextInt(topicPartitions);
    }

    return keyHash(publication.key())
             .mod(BigInteger.valueOf(topicPartitions))
             .intValue();
  }
    
  ...
```

For a not partitioned topic, no partition is assigned.

If the topic is partitioned and has a null key - partition is random. 
If a key is defined, the partition is assigned based on key hash. Thanks to this, we have a guarantee that events associated with the same key always land in the same partition.

If you want to change this behavior, you can provide your own implementation and configure it by calling `EventSQLPublisher.configurePartitioner` method.

### Consuming

We can have both single event and batch consumers:
```java
var consumers = eventSQL.consumers();

consumers.startConsumer("txt_topic", "single-consumer", event -> {
  // handle single event
});
// with more frequent polling - by default it is 1 second
consumers.startConsumer("txt_topic", "single-consumer-customized", event -> {
  // handle single event
}, Duration.ofMillis(100));

consumers.startBatchConsumer("txt_topic", "batch-consumer", events -> {
  // handle events batch for better performance
}, // customize batch behavior:
  // minEvents, maxEvents,
  // pollingDelay and maxPollingDelay - how long to wait for minEvents
  EventSQLConsumers.ConsumptionConfig.of(5, 100,
    Duration.ofSeconds(1), Duration.ofSeconds(10)));
```

### Dead Letter Topics (DLT)

If we register a topic with DLT as follows:
```java
eventSQL.registry()
  .registerTopic(new TopicDefinition("account_created", -1))
  .registerTopic(new TopicDefinition("account_created_dlt", -1));
```
Under certain circumstances, it will get a special treatment.

When a consumer throws `EventSQLConsumptionException`, `DefaultDLTEventFactory` takes over and publishes failed event to the associated DLT, if it can find one:
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

This factory can be customized by calling `EventSQLConsumers.configureDLTEventFactory` method.

What is also worth noting is that any exception thrown by a single event consumer is wrapped into `EventSQLConsumptionException` automatically - see `ConsumerWrapper` class.

When you use `EventSQLConsumers.startBatchConsumer` you have to do the wrapping yourself.


## How to get it

Maven:
```
TODO: publish it
```
Gradle:
```
TODO: publish it
```