package com.binaryigor.eventsql.internal.sharded;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLPublisher;

import java.util.Collection;
import java.util.List;
import java.util.Random;

public class ShardedEventSQLPublisher implements EventSQLPublisher {

    private final static Random RANDOM = new Random();
    private final List<EventSQLPublisher> publishers;

    public ShardedEventSQLPublisher(List<EventSQLPublisher> publishers) {
        this.publishers = publishers;
    }

    @Override
    public void publish(EventPublication publication) {
        nextPublisher().publish(publication);
    }

    private EventSQLPublisher nextPublisher() {
        return publishers.get(RANDOM.nextInt(publishers.size()));
    }

    @Override
    public void publishAll(Collection<EventPublication> publications) {
        nextPublisher().publishAll(publications);
    }
}
