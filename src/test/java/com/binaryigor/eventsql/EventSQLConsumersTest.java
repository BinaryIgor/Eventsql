package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.IntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import com.binaryigor.eventsql.test.Tests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.binaryigor.eventsql.test.Tests.awaitAssertion;
import static org.assertj.core.api.Assertions.assertThat;

public class EventSQLConsumersTest extends IntegrationTest {

    private static final String TOPIC = "topic";
    private static final String PARTITIONED_TOPIC = "partitioned_topic";
    private static final String WITH_DLT_TOPIC = "with_dlt_topic";
    private static final String WITH_DLT_DLT_TOPIC = WITH_DLT_TOPIC + "_dlt";

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(TOPIC, -1))
                .registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3))
                .registerTopic(new TopicDefinition(WITH_DLT_TOPIC, -1))
                .registerTopic(new TopicDefinition(WITH_DLT_DLT_TOPIC, -1));
    }

    @Test
    void consumesEventsFromTopic() {
        // given
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);
        var event1 = TestObjects.randomEventPublication(TOPIC);
        var event2 = TestObjects.randomEventPublication(TOPIC);
        var event3 = TestObjects.randomEventPublication(TOPIC);
        var capturedEvents = new ArrayList<Event>();

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), capturedEvents::add);
        publisher.publish(event1);
        publisher.publish(event2);
        publisher.publish(event3);

        // then
        awaitAssertion(() -> assertExpectedEvents(capturedEvents, event1, event2, event3));
    }

    @Test
    void consumesEventsFromPartitionedTopic() {
        // given
        var consumer = new ConsumerDefinition(PARTITIONED_TOPIC, "test-consumer", true);
        registry.registerConsumer(consumer);
        var event1P0 = TestObjects.randomEventPublication(PARTITIONED_TOPIC, 0);
        var event2P0 = TestObjects.randomEventPublication(PARTITIONED_TOPIC, 0);
        var event1P1 = TestObjects.randomEventPublication(PARTITIONED_TOPIC, 1);
        var event1P2 = TestObjects.randomEventPublication(PARTITIONED_TOPIC, 2);
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
        });
        publisher.publish(event1P0);
        publisher.publish(event2P0);
        publisher.publish(event1P1);
        publisher.publish(event1P2);

        // then
        awaitAssertion(() -> {
            assertExpectedEvents(p0CapturedEvents, event1P0, event2P0);
            assertExpectedEvents(p1CapturedEvents, event1P1);
            assertExpectedEvents(p2CapturedEvents, event1P2);
        });
    }

    @Test
    void consumesEventsInBatches() {
        // given
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);
        var toPublishBatch1 = Stream.generate(() -> TestObjects.randomEventPublication(TOPIC)).limit(5).toList();
        var toPublishBatch2 = Stream.generate(() -> TestObjects.randomEventPublication(TOPIC)).limit(10).toList();
        var capturedBatches = new ArrayList<Collection<Event>>();

        // when
        consumers.startBatchConsumer(consumer.topic(), consumer.name(), capturedBatches::add,
                EventSQLConsumers.ConsumptionConfig.of(5, 10,
                        Duration.ofMillis(10), Duration.ofSeconds(1)));
        publisher.publishAll(toPublishBatch1);
        publisher.publishAll(toPublishBatch2);

        // then
        awaitAssertion(() -> {
            assertThat(capturedBatches).hasSizeGreaterThan(1);
            capturedBatches.forEach(batch -> assertThat(batch).hasSizeGreaterThan(1));

            var capturedEvents = capturedBatches.stream().flatMap(Collection::stream).toList();
            var publishedEvents = new ArrayList<>(toPublishBatch1);
            publishedEvents.addAll(toPublishBatch2);

            assertExpectedEventsList(capturedEvents, publishedEvents);
        });
    }

    @Test
    void onFailureForTopicWithoutDltConsumptionIsStuckOnFailedEvent() {
        // given
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);
        var event1 = TestObjects.randomEventPublication(TOPIC, "event1");
        var event2 = TestObjects.randomEventPublication(TOPIC, "event2Failure");
        var event3 = TestObjects.randomEventPublication(TOPIC, "event3");
        var capturedEventKeys = Collections.synchronizedSet(new LinkedHashSet<String>());

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), e -> {
            capturedEventKeys.add(e.key());
            if (e.key().contains("Failure")) {
                throw new RuntimeException("Failure!");
            }
        }, Duration.ofMillis(10));
        publisher.publish(event1);
        publisher.publish(event2);
        publisher.publish(event3);

        // then
        delay(500);
        assertThat(capturedEventKeys)
                .containsOnly("event1", "event2Failure");
    }

    @Test
    void onFailureForTopicWithDltEventIsPublishedToDlt() {
        // given
        var consumer = new ConsumerDefinition(WITH_DLT_TOPIC, "test-consumer", false);
        var dltConsumer = new ConsumerDefinition(WITH_DLT_DLT_TOPIC, "dlt-" + consumer.name(), false);
        registry.registerConsumer(consumer);
        registry.registerConsumer(dltConsumer);

        var event1 = TestObjects.randomEventPublication(WITH_DLT_TOPIC, "event1");
        var event2Failure = TestObjects.randomEventPublication(WITH_DLT_TOPIC, "event2Failure");
        var event3Failure = TestObjects.randomEventPublication(WITH_DLT_TOPIC, "event3Failure");
        var event4 = TestObjects.randomEventPublication(WITH_DLT_TOPIC, "event4");
        var capturedEvents = new LinkedHashSet<Event>();
        var capturedDltEvents = new ArrayList<Event>();

        consumers.startConsumer(dltConsumer.topic(), dltConsumer.name(), capturedDltEvents::add);

        // when
        var exception = new RuntimeException("Failure!");
        consumers.startConsumer(consumer.topic(), consumer.name(), e -> {
            capturedEvents.add(e);
            if (e.key().contains("Failure")) {
                throw exception;
            }
        });
        publisher.publish(event1);
        publisher.publish(event2Failure);
        publisher.publish(event3Failure);
        publisher.publish(event4);

        // then
        awaitAssertion(() -> assertExpectedEvents(capturedEvents, event1, event2Failure, event3Failure, event4));
        // and then failed events were published to dlt
        awaitAssertion(() -> {
            var expectedDltEvent1 = toDltEvent(exception, event2Failure, consumer.name());
            var expectedDltEvent2 = toDltEvent(exception, event3Failure, consumer.name());
            assertExpectedEvents(capturedDltEvents, expectedDltEvent1, expectedDltEvent2);
        });
    }

    @Test
    void consumesEventsInLoop() {
        // given
        var consumer = new ConsumerDefinition(TOPIC, "test-consumer", false);
        registry.registerConsumer(consumer);
        var capturedEvents = new ArrayList<Event>();
        var eventsToPublish = 50;

        // when
        consumers.startConsumer(consumer.topic(), consumer.name(), capturedEvents::add, Duration.ofMillis(10));

        IntStream.range(0, eventsToPublish)
                .forEach($ -> {
                    publisher.publish(TestObjects.randomEventPublication(TOPIC));
                    someDelay();
                });

        // then
        awaitAssertion(() -> assertThat(capturedEvents).hasSize(eventsToPublish));
    }

    private void assertExpectedEvents(Collection<Event> capturedEvents, EventPublication... expectations) {
        assertExpectedEventsList(capturedEvents, List.of(expectations));
    }

    private void assertExpectedEventsList(Collection<Event> capturedEvents, List<EventPublication> expectations) {
        assertThat(capturedEvents)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactlyElementsOf(expectedEvents(expectations));
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

    private void someDelay() {
        delay(10);
    }

    private void delay(int millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private EventPublication toDltEvent(Throwable thrownException, EventPublication publication, String consumer) {
        return dltEventFactory.create(new EventSQLConsumptionException(thrownException, toEvent(publication)), consumer).orElseThrow();
    }
}
