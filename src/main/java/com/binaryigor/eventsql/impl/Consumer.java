package com.binaryigor.eventsql.impl;

import java.time.Instant;

public record Consumer(String topic,
                       String name,
                       int partition,
                       Long lastEventId,
                       Instant lastConsumptionAt) {
}
