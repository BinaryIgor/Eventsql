package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class EventsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(EventsConsumer.class);
    private final EventSQL eventSQL;
    private final ObjectMapper objectMapper;
    private final String accountCreatedTopic;
    private final String accountCreatedConsumer;
    private final AccountCreatedHandler accountCreatedHandler;

    public EventsConsumer(EventSQL eventSQL,
                          ObjectMapper objectMapper,
                          @Value("${events.account-created-topic}")
                          String accountCreatedTopic,
                          @Value("${events.account-created-consumer}")
                          String accountCreatedConsumer,
                          AccountCreatedHandler accountCreatedHandler) {
        this.eventSQL = eventSQL;
        this.objectMapper = objectMapper;
        this.accountCreatedTopic = accountCreatedTopic;
        this.accountCreatedConsumer = accountCreatedConsumer;
        this.accountCreatedHandler = accountCreatedHandler;
    }

    @PostConstruct
    void start() {
        eventSQL.registry()
                .registerTopic(new TopicDefinition(accountCreatedTopic, 10))
                .registerTopic(new TopicDefinition(accountCreatedTopic +"_dlt", 10))
                .registerConsumer(new ConsumerDefinition(accountCreatedTopic, accountCreatedConsumer, true));
        eventSQL.consumers()
                .startConsumer(accountCreatedTopic, accountCreatedConsumer, this::handleAccountCreatedEvent);
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
            throw e;
        } catch (Exception e) {
            logger.error("Problem while handling AccountCreated event:", e);
            throw new RuntimeException(e);
        }
    }
}
