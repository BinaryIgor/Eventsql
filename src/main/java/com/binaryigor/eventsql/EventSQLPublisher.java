package com.binaryigor.eventsql;

import java.util.Collection;

public interface EventSQLPublisher {

    void publish(EventPublication publication);

    void publishAll(Collection<EventPublication> publications);

    void configurePartitioner(Partitioner partitioner);

    Partitioner partitioner();
}
