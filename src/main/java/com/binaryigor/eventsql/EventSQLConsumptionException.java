package com.binaryigor.eventsql;

public class EventSQLConsumptionException extends RuntimeException {

    private final Event event;

    public EventSQLConsumptionException(String message, Throwable cause, Event event) {
        super(message, cause);
        this.event = event;
    }

    public EventSQLConsumptionException(Throwable cause, Event event) {
        this(null, cause, event);
    }

    public Event event() {
        return event;
    }
}
