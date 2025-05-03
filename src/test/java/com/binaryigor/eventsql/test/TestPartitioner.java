package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLPublisher;

public class TestPartitioner implements EventSQLPublisher.Partitioner {

    private final int nextPartition;

    public TestPartitioner(int nextPartition) {
        this.nextPartition = nextPartition;
    }

    @Override
    public int partition(EventPublication publication, int topicPartitions) {
        return nextPartition;
    }
}
