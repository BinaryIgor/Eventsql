package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLPublisher;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EventsPublisher {

    private static final Logger logger = LoggerFactory.getLogger(EventsPublisher.class);
    private final EventSQLPublisher eventSQLPublisher;
    private final ObjectMapper objectMapper;
    private final String accountCreatedTopic;

    public EventsPublisher(EventSQLPublisher eventSQLPublisher,
                           ObjectMapper objectMapper,
                           EventsProperties eventsProperties) {
        this.eventSQLPublisher = eventSQLPublisher;
        this.objectMapper = objectMapper;
        this.accountCreatedTopic = eventsProperties.accountCreatedTopic().name();
    }

    public void publishAccountCreated(AccountCreated accountCreated) {
        try {
            var eventBytes = objectMapper.writeValueAsBytes(accountCreated);
            eventSQLPublisher.publish(new EventPublication(accountCreatedTopic, eventBytes));
        } catch (Exception e) {
            logger.error("Fail to publish AccountCreated event: ", e);
            throw new RuntimeException(e);
        }
    }
}
