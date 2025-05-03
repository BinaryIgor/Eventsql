package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.Partitioner;

public class TestPartitioner implements Partitioner {

    private final int nextPartition;

    public TestPartitioner(int nextPartition) {
        this.nextPartition = nextPartition;
    }

    @Override
    public int partition(EventPublication publication, int topicPartitions) {
        return nextPartition;
    }
}
