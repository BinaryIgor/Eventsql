package com.binaryigor.eventsql.internal;

public record EventSequence(String topic, int partition, long nextSeq) {

    public EventSequence incremented() {
        return new EventSequence(topic, partition, nextSeq + 1);
    }
}
