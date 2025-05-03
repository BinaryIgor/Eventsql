package com.binaryigor.eventsql.internal.sharded;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventSQLConsumers;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public class ShardedEventSQLConsumers implements EventSQLConsumers {

    private final List<EventSQLConsumers> consumers;

    public ShardedEventSQLConsumers(List<EventSQLConsumers> consumers) {
        this.consumers = consumers;
    }

    @Override
    public void startConsumer(String topic, String name, Consumer<Event> consumer) {
        consumers.forEach(c -> c.startConsumer(topic, name, consumer));
    }

    @Override
    public void startConsumer(String topic, String name, Consumer<Event> consumer, Duration pollingDelay) {
        consumers.forEach(c -> c.startConsumer(topic, name, consumer, pollingDelay));
    }

    @Override
    public void startConsumer(String topic, String name, Consumer<Event> consumer, Duration pollingDelay, int maxInMemoryEvents) {
        consumers.forEach(c -> c.startConsumer(topic, name, consumer, pollingDelay, maxInMemoryEvents));
    }

    @Override
    public void startBatchConsumer(String topic, String name, Consumer<List<Event>> consumer, ConsumptionConfig consumptionConfig) {
        consumers.forEach(c -> c.startBatchConsumer(topic, name, consumer, consumptionConfig));
    }

    @Override
    public void configureDLTEventFactory(DLTEventFactory dltEventFactory) {
        consumers.forEach(c -> c.configureDLTEventFactory(dltEventFactory));
    }

    @Override
    public DLTEventFactory dltEventFactory() {
        return consumers.getFirst().dltEventFactory();
    }

    @Override
    public void stop(Duration timeout) {
        var stopThreads = consumers.stream()
                .map(c -> Thread.startVirtualThread(() -> c.stop(timeout)))
                .toList();

        stopThreads.forEach(t -> {
            try {
                t.join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public List<EventSQLConsumers> consumers() {
        return consumers;
    }
}
