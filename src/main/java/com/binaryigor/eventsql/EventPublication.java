package com.binaryigor.eventsql;

import java.util.Map;

public record EventPublication(String topic,
                               int partition,
                               String key,
                               byte[] value,
                               Map<String, String> metadata) {

    public EventPublication(String topic, int partition, String key, byte[] value) {
        this(topic, partition, key, value, Map.of());
    }

    public EventPublication(String topic, int partition, byte[] value, Map<String, String> metadata) {
        this(topic, partition, null, value, metadata);
    }

    public EventPublication(String topic, int partition, byte[] value) {
        this(topic, partition, value, Map.of());
    }

    public EventPublication(String topic, String key, byte[] value, Map<String, String> metadata) {
        this(topic, -1, key, value, metadata);
    }

    public EventPublication(String topic, String key, byte[] value) {
        this(topic, key, value, Map.of());
    }

    public EventPublication(String topic, byte[] value, Map<String, String> metadata) {
        this(topic, null, value, metadata);
    }

    public EventPublication(String topic, byte[] value) {
        this(topic, value, Map.of());
    }

    public EventPublication withPartition(int partition) {
        return new EventPublication(topic, partition, key, value, metadata);
    }
}
