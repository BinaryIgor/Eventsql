package com.binaryigor.eventsql;

import java.util.Map;

public record Event(String topic,
                    long id,
                    int partition,
                    String key,
                    byte[] value,
                    Map<String, String> metadata) {

    public Event(String topic, long id, int partition, byte[] value, Map<String, String> metadata) {
        this(topic, id, partition, null, value, metadata);
    }
}
