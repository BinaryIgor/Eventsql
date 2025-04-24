package com.binaryigor.eventsql;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

public interface EventSQLConsumers {

    Duration DEFAULT_POLLING_DELAY = Duration.ofSeconds(1);
    Duration DEFAULT_MAX_POLLING_DELAY = Duration.ofSeconds(30);
    int DEFAULT_MIN_EVENTS = 10;
    int DEFAULT_MAX_EVENTS = 100;

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer);

    void startConsumer(String topic, String name,
                       Consumer<Event> consumer,
                       Duration pollingDelay);

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

        public static ConsumptionConfig BATCH_DEFAULTS = new ConsumptionConfig(DEFAULT_MIN_EVENTS, DEFAULT_MAX_EVENTS, DEFAULT_POLLING_DELAY, DEFAULT_MAX_POLLING_DELAY);
    }

    interface DLTEventFactory {
        Optional<EventPublication> create(EventSQLConsumptionException exception, String consumer);
    }
}
