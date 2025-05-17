package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.ConsumerDefinition;
import com.binaryigor.eventsql.EventSQLRegistry;
import com.binaryigor.eventsql.TopicDefinition;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

// TODO: remove partitioned consumer options - it should be derived from a topic
public class DefaultEventSQLRegistry implements EventSQLRegistry {

    private final TopicRepository topicRepository;
    private final EventSequenceRepository eventSequenceRepository;
    private final EventRepository eventRepository;
    private final ConsumerRepository consumerRepository;
    private final Transactions transactions;

    public DefaultEventSQLRegistry(TopicRepository topicRepository,
                                   EventRepository eventRepository,
                                   EventSequenceRepository eventSequenceRepository,
                                   ConsumerRepository consumerRepository,
                                   Transactions transactions) {
        this.topicRepository = topicRepository;
        this.eventSequenceRepository = eventSequenceRepository;
        this.eventRepository = eventRepository;
        this.consumerRepository = consumerRepository;
        this.transactions = transactions;
    }

    // TODO: support more complex modifications
    @Override
    public EventSQLRegistry registerTopic(TopicDefinition topic) {
        if (topic.partitions() != -1 && topic.partitions() <= 0) {
            throw new IllegalArgumentException("Topic can be either not partitioned (-1) or must have at least 1 partition, but %s was given"
                    .formatted(topic.partitions()));
        }

        var currentTopicDefinitionOpt = topicRepository.ofName(topic.name());
        if (currentTopicDefinitionOpt.isEmpty()) {
            transactions.execute(() -> {
                topicRepository.save(topic);
                eventSequenceRepository.saveAll(initialEventSequences(topic));
                eventRepository.createPartition(topic.name());
            });
            return this;
        }

        if (currentTopicDefinitionOpt.get().equals(topic)) {
            return this;
        }

        if (topicHasEventsOrConsumers(topic.name())) {
            throw new IllegalArgumentException("%s topic has events or consumers - if you want to modify it, delete them first"
                    .formatted(topic.name()));
        }

        topicRepository.save(topic);
        eventSequenceRepository.saveAll(initialEventSequences(topic));

        return this;
    }

    private Collection<EventSequence> initialEventSequences(TopicDefinition topic) {
        if (topic.partitions() == -1) {
            return List.of(new EventSequence(topic.name(), -1, 1));
        }
        return IntStream.range(0, topic.partitions())
                .mapToObj(p -> new EventSequence(topic.name(), p, 1))
                .toList();
    }

    private boolean topicHasEventsOrConsumers(String topic) {
        return !eventRepository.nextEvents(topic, null, 1).isEmpty() || !consumerRepository.allOf(topic).isEmpty();
    }

    @Override
    public EventSQLRegistry unregisterTopic(String topic) {
        transactions.execute(() -> {
            var topicConsumers = consumerRepository.allOf(topic);
            if (!topicConsumers.isEmpty()) {
                throw new IllegalArgumentException("Cannot unregister topic with consumers. Unregister them first");
            }

            topicRepository.delete(topic);
            eventRepository.deletePartition(topic);
            eventSequenceRepository.deleteAllOfTopic(topic);
        });

        return this;
    }

    @Override
    public List<TopicDefinition> listTopics() {
        return topicRepository.all();
    }

    @Override
    public EventSQLRegistry registerConsumer(ConsumerDefinition consumer) {
        var topic = findTopicDefinition(consumer.topic());

        var currentConsumers = consumerRepository.allOf(consumer.topic(), consumer.name());
        if (currentConsumers.size() == topic.partitions() ||
                (topic.partitions() == -1 && currentConsumers.size() == 1)) {
            return this;
        }

        currentConsumers.forEach(c -> {
            if (c.lastEventSeq() != null) {
                throw new IllegalArgumentException("Cannot modify consumers with state; you must unregister them first");
            }
        });

        transactions.execute(() -> {
            consumerRepository.deleteAllOf(consumer.topic(), consumer.name());
            var consumersToSave = toConsumers(consumer, topic);
            consumerRepository.saveAll(consumersToSave);
        });
        return this;
    }

    @Override
    public EventSQLRegistry unregisterConsumer(String topic, String name) {
        consumerRepository.deleteAllOf(topic, name);
        return this;
    }

    @Override
    public List<ConsumerDefinition> listConsumers() {
        var groupedConsumers = consumerRepository.all().stream()
                .collect(Collectors.groupingBy(e -> e.topic() + e.name(), LinkedHashMap::new, toList()));

        return groupedConsumers.values().stream()
                .map(consumers -> {
                    var first = consumers.getFirst();
                    return new ConsumerDefinition(first.topic(), first.name());
                })
                .toList();
    }

    private TopicDefinition findTopicDefinition(String topic) {
        return topicRepository.ofName(topic)
                .orElseThrow(() -> new IllegalArgumentException("%s topic doesn't exist".formatted(topic)));
    }

    private List<Consumer> toConsumers(ConsumerDefinition registration,
                                       TopicDefinition topic) {
        if (topic.partitions() == -1) {
            return List.of(toConsumer(registration, -1));
        }
        return IntStream.range(0, topic.partitions())
                .mapToObj(partition -> toConsumer(registration, partition))
                .toList();
    }

    private Consumer toConsumer(ConsumerDefinition registration, int partition) {
        return new Consumer(registration.topic(), registration.name(), partition, null, null);
    }
}
