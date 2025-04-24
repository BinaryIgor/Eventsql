package com.binaryigor.eventsql;

public record ConsumerDefinition(String topic, String name, boolean partitioned) {
}
