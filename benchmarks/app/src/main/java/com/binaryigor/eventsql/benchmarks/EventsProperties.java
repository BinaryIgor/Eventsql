package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.ConsumerDefinition;
import com.binaryigor.eventsql.TopicDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("events")
public record EventsProperties(TopicDefinition accountCreatedTopic,
                               TopicDefinition accountCreatedTopicDlt,
                               ConsumerDefinition accountCreatedConsumer) {
}
