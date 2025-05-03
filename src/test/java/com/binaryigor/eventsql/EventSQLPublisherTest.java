package com.binaryigor.eventsql;

import com.binaryigor.eventsql.test.IntegrationTest;
import com.binaryigor.eventsql.test.TestObjects;
import com.binaryigor.eventsql.test.TestPartitioner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    void publishesToVariousPartitions() {
        // when
        IntStream.range(0, 25)
                .forEach(idx -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC)));

        // then
        assertThat(publishedEvents())
                .extracting("partition")
                .contains(0, 1, 2);
    }

    @Test
    void publishesBatchToVariousPartitions() {
        // when
        var toPublishEvents = Stream.generate(() -> TestObjects.randomEventPublication(PARTITIONED_TOPIC))
                .limit(50)
                .toList();
        publisher.publishAll(toPublishEvents);

        // then
        assertThat(publishedEvents())
                .extracting("partition")
                .contains(0, 1, 2);
    }

    @Test
    void doesNotAllowToPublishToNonExistingTopic() {
        // expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication("non_existing")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("topic of non_existing name doesn't exist");
    }

    @ParameterizedTest
    @ValueSource(ints = {-99, -10, -2})
    void doesNotAllowToPublishEventToIllegalPartitionValues(int illegalValue) {
        // given
        publisher.configurePartitioner(new TestPartitioner(illegalValue));

        //expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Illegal partition value: " + illegalValue);
    }

    @Test
    void doesNotAllowToPublishPartitionedEventToNotPartitionedTopic() {
        //given
        publisher.configurePartitioner(new TestPartitioner(1));

        // expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication(NOT_PARTITIONED_TOPIC)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NOT_PARTITIONED_TOPIC + " topic is not partitioned, but publication to 1 partition was requested");
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 10, 101})
    void doesNotAllowToPublishEventToPartitionOutsideAllowedByTopicDefinitionValues(int outsideValue) {
        // given
        publisher.configurePartitioner(new TestPartitioner(outsideValue));

        // expect
        assertThatThrownBy(() -> publisher.publish(TestObjects.randomEventPublication(PARTITIONED_TOPIC)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(PARTITIONED_TOPIC + " topic has only %d partitions, but publishing to %d was requested"
                        .formatted(3, outsideValue));
    }

    private List<Event> publishedEvents() {
        return eventRepository.nextEvents(PARTITIONED_TOPIC, null, Integer.MAX_VALUE);
    }
}
