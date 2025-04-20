package com.binaryigor.eventsql.test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

public class TestClock extends Clock {

    public final ZoneId zoneId = ZoneId.of("UTC");
    private Instant time;

    public TestClock(Instant time) {
        this.time = time;
    }

    public TestClock() {
        this(Instant.now());
    }

    public void time(Instant time) {
        this.time = time;
    }

    @Override
    public ZoneId getZone() {
        return zoneId;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return null;
    }

    @Override
    public Instant instant() {
        return time;
    }
}
