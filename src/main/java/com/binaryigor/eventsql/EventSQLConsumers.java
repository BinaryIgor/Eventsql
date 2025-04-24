package com.binaryigor.eventsql;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

public interface EventSQLConsumers {

    Duration DEFAULT_POLLING_DELAY = Duration.ofSeconds(1);
    int DEFAULT_IN_MEMORY_EVENTS = 50;

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer);

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer,
                       Duration pollingDelay);

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer,
                       Duration pollingDelay,
                       int maxInMemoryEvents);

    void startBatchConsumer(String topic, String name,
                            Consumer<Collection<Event>> consumer,
                            ConsumptionConfig consumptionConfig);

    void configureDLTEventFactory(DLTEventFactory dltEventFactory);

    void stop(Duration timeout);

    record ConsumptionConfig(
            int minEvents,
            int maxEvents,
            Duration pollingDelay,
            Duration maxPollingDelay) {

        public static ConsumptionConfig of(int minEvents,
                                           int maxEvents,
                                           Duration pollingDelay,
                                           Duration maxPollingDelay) {
            return new ConsumptionConfig(minEvents, maxEvents, pollingDelay, maxPollingDelay);
        }
    }

    interface DLTEventFactory {
        Optional<EventPublication> create(EventSQLConsumptionException exception, String consumer);
    }
}
