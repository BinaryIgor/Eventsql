package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.ShardedIntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static com.binaryigor.eventsql.test.Tests.awaitAssertion;
import static org.assertj.core.api.Assertions.assertThat;

public class ShardedEventSQLConsumersTest extends ShardedIntegrationTest {

    private static final String TOPIC = "topic";
    private static final String PARTITIONED_TOPIC = "partitioned_topic";
    private static final Duration CONSUMER_POLLING_DELAY = Duration.ofMillis(10);

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(TOPIC, -1))
                .registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3));
    }

    @Test
    void consumesEventsFromTopicOnAllShards() {
        // given
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);

        var events = Stream.generate(() -> TestObjects.randomEventPublication(TOPIC))
                .limit(10)
                .toList();
        var capturedEvents = new ArrayList<Event>();

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), capturedEvents::add, CONSUMER_POLLING_DELAY);
        events.forEach(publisher::publish);

        // then
        awaitAssertion(() -> assertExpectedEvents(capturedEvents, events));
    }

    @Test
    void consumesEventsFromPartitionedTopicOnAllShards() {
        // given
        var consumer = new ConsumerDefinition(PARTITIONED_TOPIC, "test-consumer", true);
        registry.registerConsumer(consumer);

        var p0Events = Stream.generate(() -> TestObjects.randomEventPublication(PARTITIONED_TOPIC, 0))
                .limit(5)
                .toList();
        var p1Events = Stream.generate(() -> TestObjects.randomEventPublication(PARTITIONED_TOPIC, 1))
                .limit(10)
                .toList();
        var p2Events = Stream.generate(() -> TestObjects.randomEventPublication(PARTITIONED_TOPIC, 2))
                .limit(15)
                .toList();
        var p0CapturedEvents = new ArrayList<Event>();
        var p1CapturedEvents = new ArrayList<Event>();
        var p2CapturedEvents = new ArrayList<Event>();

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), e -> {
            if (e.partition() == 0) {
                p0CapturedEvents.add(e);
            } else if (e.partition() == 1) {
                p1CapturedEvents.add(e);
            } else {
                p2CapturedEvents.add(e);
            }
        }, CONSUMER_POLLING_DELAY);
        p0Events.forEach(publisher::publish);
        p1Events.forEach(publisher::publish);
        p2Events.forEach(publisher::publish);

        // then
        awaitAssertion(() -> {
            assertExpectedEvents(p0CapturedEvents, p0Events);
            assertExpectedEvents(p1CapturedEvents, p1Events);
            assertExpectedEvents(p2CapturedEvents, p2Events);
        });
    }

    private void assertExpectedEvents(Collection<Event> capturedEvents, List<EventPublication> expectations) {
        assertThat(capturedEvents)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyInAnyOrderElementsOf(expectedEvents(expectations));
    }

    private List<Event> expectedEvents(List<EventPublication> publications) {
        return publications.stream()
                .map(this::toEvent)
                .toList();
    }

    private Event toEvent(EventPublication publication) {
        return new Event(publication.topic(), -1, publication.partition(), publication.key(), publication.value(),
                publication.metadata());
    }
}
