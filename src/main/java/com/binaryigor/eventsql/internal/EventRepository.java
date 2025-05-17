package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.Event;

import java.util.Collection;
import java.util.List;

public interface EventRepository {

    void createPartition(String topic);

    void deletePartition(String topic);

    void create(EventInput event);

    void createAll(Collection<EventInput> events);

    List<Event> nextEvents(String topic, Long lastSeq, int limit);

    List<Event> nextEvents(String topic, int partition, Long lastSeq, int limit);
}
