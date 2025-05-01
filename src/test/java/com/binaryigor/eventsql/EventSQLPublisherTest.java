package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.IntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class EventSQLPublisherTest extends IntegrationTest {

    private static final String PARTITIONED_TOPIC = "partitioned_topic";
    private static final String NOT_PARTITIONED_TOPIC = "not_partitioned_topic";

    @BeforeEach
    void setup() {
        registry.registerTopic(new TopicDefinition(PARTITIONED_TOPIC, 3))
                .registerTopic(new TopicDefinition(NOT_PARTITIONED_TOPIC, -1));
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

    @ParameterizedTest
    @ValueSource(ints = {-99, -10, -2})
    void doesNotAllowToPublishEventToIllegalPartitionValues(int illegalValue) {
        // expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC, illegalValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Illegal partition value: " + illegalValue);
    }

    @Test
    void doesNotAllowToPublishPartitionedEventToNotPartitionedTopic() {
        // expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication(NOT_PARTITIONED_TOPIC, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NOT_PARTITIONED_TOPIC + " topic is not partitioned, but publication to 1 partition was requested");
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 10, 101})
    void doesNotAllowToPublishEventToPartitionOutsideAllowedByTopicDefinitionValues(int outsideValue) {
        // expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC, outsideValue)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(PARTITIONED_TOPIC + " topic has only %d partitions, but publishing to %d was requested"
                        .formatted(3, outsideValue));
    }

    private List<Event> publishedEvents(String topic) {
        return eventRepository.nextEvents(topic, null, Integer.MAX_VALUE);
    }
}
