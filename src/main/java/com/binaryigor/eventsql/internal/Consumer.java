package com.binaryigor.eventsql.internal;

import java.time.Instant;

public record Consumer(String topic,
                       String name,
                       int partition,
                       Long lastEventId,
                       Instant lastConsumptionAt) {
}
