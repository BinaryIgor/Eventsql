package com.binaryigor.eventsql.impl;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventSQLConsumptionException;

import java.util.Collection;
import java.util.function.Consumer;

public class ConsumerWrapper implements Consumer<Collection<Event>> {

    private final Consumer<Event> wrapped;

    public ConsumerWrapper(Consumer<Event> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void accept(Collection<Event> events) {
        events.forEach(e -> {
            try {
                wrapped.accept(e);
            } catch (Throwable t) {
                throw new EventSQLConsumptionException(t, e);
            }
        });
    }
}
