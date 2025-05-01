package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.ShardedIntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ShardedEventSQLPublisherTest extends ShardedIntegrationTest {

    private static final String PARTITIONED_TOPIC = "partitioned_topic";

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3));
    }

    @Test
    void publishesToRandomPartitionToAllShards() {
        // when
        IntStream.range(0, 50)
                .forEach($ -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC, -1)));

        // then
        assertOnEachShard(shard -> {
            var publishedEvents = publishedEvents(PARTITIONED_TOPIC, shard);
            assertThat(publishedEvents)
                    .extracting("partition")
                    .contains(0, 1, 2);
        });
    }

    @Test
    void publishesToSpecifiedPartitionToAllShards() {
        // when
        IntStream.range(0, 10)
                .forEach($ -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC, 1)));

        // then
        assertOnEachShard(shard -> assertThat(publishedEvents(PARTITIONED_TOPIC, shard))
                .extracting("partition")
                .containsOnly(1));
    }

    @Test
    void publishesBatchesToAllShards() {
        // when
        IntStream.range(0, 10)
                .forEach($ -> {
                    var batch = Stream.generate(() -> TestObjects.randomEventPublication(PARTITIONED_TOPIC, 1))
                            .limit(5)
                            .toList();
                    publisher.publishAll(batch);
                });

        // then
        assertOnEachShard(shard -> {
            assertThat(publishedEvents(PARTITIONED_TOPIC, shard))
                    .extracting("partition")
                    .containsOnly(1);
        });
    }

    private void assertOnEachShard(Consumer<Integer> shardAssertion) {
        IntStream.range(0, SHARDS)
                .forEach(shardAssertion::accept);
    }

    private List<Event> publishedEvents(String topic, int shard) {
        return eventRepositories.get(shard).nextEvents(topic, null, Integer.MAX_VALUE);
    }
}
