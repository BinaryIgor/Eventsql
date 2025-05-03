package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLConsumptionException;

import java.util.LinkedHashMap;
import java.util.Optional;

public class DefaultDLTEventFactory implements EventSQLConsumers.DLTEventFactory {

    private final TopicDefinitionsCache topicDefinitionsCache;

    public DefaultDLTEventFactory(TopicDefinitionsCache topicDefinitionsCache) {
        this.topicDefinitionsCache = topicDefinitionsCache;
    }

    @Override
    public Optional<EventPublication> create(EventSQLConsumptionException exception, String consumer) {
        var event = exception.event();

        var dltTopic = event.topic() + "_dlt";
        // optimization: do not ask for dlts of topic without them, every single time
        var dltTopicDefinitionOpt = topicDefinitionsCache.getLoadingIf(dltTopic, true);
        if (dltTopicDefinitionOpt.isEmpty()) {
            return Optional.empty();
        }

        var metadata = new LinkedHashMap<>(event.metadata());
        metadata.put("consumerName", consumer);

        var cause = exception.getCause();
        metadata.put("exceptionType", cause.getClass().getName());
        metadata.put("exceptionMessage", Optional.ofNullable(cause.getMessage()).orElse(exception.getMessage()));

        return Optional.of(new EventPublication(dltTopic, event.key(), event.value(), metadata));
    }
}
