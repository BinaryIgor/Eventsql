package com.binaryigor.eventsql;

import java.util.Map;

public record EventPublication(String topic,
                               String key,
                               byte[] value,
                               Map<String, String> metadata) {

    public EventPublication(String topic, String key, byte[] value) {
        this(topic, key, value, Map.of());
    }

    public EventPublication(String topic, byte[] value, Map<String, String> metadata) {
        this(topic, null, value, metadata);
    }

    public EventPublication(String topic, byte[] value) {
        this(topic, value, Map.of());
    }
}
