package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLConsumptionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class EventsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventsConsumer.class);
    private final EventSQL eventSQL;
    private final ObjectMapper objectMapper;
    private final AccountCreatedHandler accountCreatedHandler;
    private final EventsProperties eventsProperties;
    private final AtomicBoolean accountCreatedBatchHandler = new AtomicBoolean(false);

    public EventsConsumer(EventSQL eventSQL,
                          ObjectMapper objectMapper,
                          AccountCreatedHandler accountCreatedHandler,
                          EventsProperties eventsProperties) {
        this.eventSQL = eventSQL;
        this.objectMapper = objectMapper;
        this.accountCreatedHandler = accountCreatedHandler;
        this.eventsProperties = eventsProperties;
    }

    public void batchAccountCreatedHandler(boolean batch) {
        accountCreatedBatchHandler.set(batch);
    }

    @PostConstruct
    void start() {
        eventSQL.registry()
                .registerTopic(eventsProperties.accountCreatedTopic())
                .registerTopic(eventsProperties.accountCreatedTopicDlt())
                .registerConsumer(eventsProperties.accountCreatedConsumer());

        var accountCreatedTopic = eventsProperties.accountCreatedTopic().name();
        var accountCreatedConsumer = eventsProperties.accountCreatedConsumer().name();
        eventSQL.consumers()
                .startBatchConsumer(accountCreatedTopic, accountCreatedConsumer,
                        events -> {
                            if (accountCreatedBatchHandler.get()) {
                                handleAccountCreatedEvents(events);
                            } else {
                                events.forEach(this::handleAccountCreatedEvent);
                            }
                        },
                        EventSQLConsumers.ConsumptionConfig.of(10, 100));
    }

    @PreDestroy
    void stop() {
        eventSQL.consumers().stop(Duration.ofSeconds(5));
    }

    private void handleAccountCreatedEvent(Event event) {
        try {
            var accountCreated = objectMapper.readValue(event.value(), AccountCreated.class);
            accountCreatedHandler.handle(accountCreated);
        } catch (IllegalStateException e) {
            throw new EventSQLConsumptionException(e, event);
        } catch (Exception e) {
            logger.error("Problem while handling AccountCreated event:", e);
            throw new RuntimeException(e);
        }
    }

    private void handleAccountCreatedEvents(List<Event> events) {
        try {
            var accountsCreated = new ArrayList<AccountCreated>(events.size());
            for (var e : events) {
                accountsCreated.add(objectMapper.readValue(e.value(), AccountCreated.class));
            }
            accountCreatedHandler.handle(accountsCreated);
        } catch (IllegalStateException e) {
            var firstFailedEvent = events.getFirst();
            throw new EventSQLConsumptionException(e, firstFailedEvent);
        } catch (Exception e) {
            logger.error("Problem while handling AccountCreated events:", e);
            throw new RuntimeException(e);
        }
    }
}
