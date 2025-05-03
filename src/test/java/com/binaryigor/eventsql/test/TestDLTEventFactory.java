package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLConsumptionException;

import java.util.Optional;

public class TestDLTEventFactory implements EventSQLConsumers.DLTEventFactory {

    @Override
    public Optional<EventPublication> create(EventSQLConsumptionException exception, String consumer) {
        return Optional.empty();
    }
}
