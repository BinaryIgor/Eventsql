package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventPublication;

import java.util.Collection;
import java.util.List;

public interface EventRepository {

    void createPartition(String topic);

    void deletePartition(String topic);

    void create(EventPublication event);

    void createAll(Collection<EventPublication> events);

    List<Event> nextEvents(String topic, Long lastId, int limit);

    List<Event> nextEvents(String topic, int partition, Long lastId, int limit);
}
