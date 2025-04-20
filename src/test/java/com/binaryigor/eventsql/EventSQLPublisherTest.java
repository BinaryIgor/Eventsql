package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.IntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class EventSQLPublisherTest extends IntegrationTest {

    private static final String PARTITIONED_TOPIC = "partitioned_topic";

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3));
    }

    @Test
    void publishesToRandomPartition() {
        // when
        IntStream.range(0, 25)
                .forEach(idx -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC, -1)));

        // then
        var publishedEvents = publishedEvents(PARTITIONED_TOPIC);
        assertThat(publishedEvents)
                .extracting("partition")
                .contains(0, 1, 2);
    }

    @Test
    void publishesToSpecifiedPartition() {
        // when
        publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC, 1));
        // then
        assertThat(publishedEvents(PARTITIONED_TOPIC))
                .extracting("partition")
                .containsOnly(1);
    }

    private List<Event> publishedEvents(String topic) {
        return eventRepository.nextEvents(topic, null, Integer.MAX_VALUE);
    }
}
