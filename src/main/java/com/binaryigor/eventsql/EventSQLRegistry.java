package com.binaryigor.eventsql;

import java.util.List;

// TODO: initSchema method!
public interface EventSQLRegistry {

    EventSQLRegistry registerTopic(TopicDefinition topic);

    EventSQLRegistry unregisterTopic(String topic);

    List<TopicDefinition> listTopics();

    EventSQLRegistry registerConsumer(ConsumerDefinition consumer);

    EventSQLRegistry unregisterConsumer(String topic, String name);

    List<ConsumerDefinition> listConsumers();
}
