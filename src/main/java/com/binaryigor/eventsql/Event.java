package com.binaryigor.eventsql;

import java.util.Map;

public record Event(String topic,
                    int partition,
                    long seq,
                    String key,
                    byte[] value,
                    Map<String, String> metadata) {

    public Event(String topic, int partition, long seq, byte[] value, Map<String, String> metadata) {
        this(topic, partition, seq, null, value, metadata);
    }
}
