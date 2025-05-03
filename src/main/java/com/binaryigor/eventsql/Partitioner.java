package com.binaryigor.eventsql;

public interface Partitioner {
    int partition(EventPublication publication, int topicPartitions);
}
