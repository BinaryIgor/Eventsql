package com.binaryigor.eventsql.benchmarks;

import java.time.Instant;
import java.util.UUID;

public record AccountCreated(UUID id,
                             String email,
                             String name,
                             Instant createdAt) {
}
