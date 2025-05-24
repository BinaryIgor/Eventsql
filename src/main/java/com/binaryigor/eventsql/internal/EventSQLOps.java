package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// TODO: support topic & consumer definitions reloading
public class EventSQLOps implements EventSQLPublisher, EventSQLConsumers {

    private static final Logger logger = LoggerFactory.getLogger(EventSQLOps.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final TopicDefinitionsCache topicDefinitionsCache;
    private final Transactions transactions;
    private final ConsumerRepository consumerRepository;
    private final EventRepository eventRepository;
    private final Clock clock;
    private final AtomicBoolean publishThreadSet = new AtomicBoolean(false);
    private Thread publishThread;
    private final Map<ConsumerId, Thread> consumerThreads = new ConcurrentHashMap<>();
    private Partitioner partitioner;
    private DLTEventFactory dltEventFactory;
    private final int flushPublishBufferSize;
    private final Duration flushPublishBufferDelay;

    public EventSQLOps(TopicDefinitionsCache topicDefinitionsCache,
                       Transactions transactions,
                       ConsumerRepository consumerRepository,
                       EventRepository eventRepository,
                       Clock clock,
                       int flushPublishBufferSize,
                       Duration flushPublishBufferDelay) {
        this.topicDefinitionsCache = topicDefinitionsCache;
        this.transactions = transactions;
        this.consumerRepository = consumerRepository;
        this.eventRepository = eventRepository;
        this.clock = clock;
        this.flushPublishBufferSize = flushPublishBufferSize;
        this.flushPublishBufferDelay = flushPublishBufferDelay;
        this.partitioner = new DefaultPartitioner();
        this.dltEventFactory = new DefaultDLTEventFactory(topicDefinitionsCache);
    }

    @Override
    public void publish(EventPublication publication) {
        publish(publication.topic(), List.of(publication));
    }

    @Override
    public void publishAll(Collection<EventPublication> publications) {
        var publicationsByTopic = publications.stream()
                .collect(Collectors.groupingBy(EventPublication::topic));
        transactions.execute(() -> publicationsByTopic.forEach(this::publish));
    }

    private void publish(String topicName, Collection<EventPublication> publications) {
        var topic = findTopicDefinition(topicName);
        var toCreateEvents = publications.stream()
                .map(publication -> {
                    var partition = (short) partitioner.partition(publication, topic.partitions());
                    var eventInput = new EventInput(publication, partition);
                    validateNewEvent(eventInput, topic);
                    return eventInput;
                })
                .toList();
        eventRepository.createAll(toCreateEvents);
        if (!publishThreadSet.compareAndExchange(false, true)) {
            startPublishThread();
        }
    }

    private void startPublishThread() {
        publishThread = Thread.startVirtualThread(() -> {
            try {
                while (running.get()) {
                    Thread.sleep(flushPublishBufferDelay);
                    flushEventsBuffer();
                }
            } catch (Exception e) {
                logger.error("Fail to flush publish buffer", e);
            }
        });
    }

    public boolean flushEventsBuffer() {
        return eventRepository.flushBuffer(flushPublishBufferSize);
    }

    private void validateNewEvent(EventInput publication, TopicDefinition topic) {
        if (publication.partition() < -1) {
            throw new IllegalArgumentException("Illegal partition value: " + publication.partition());
        } else if (topic.partitions() == -1 && publication.partition() != -1) {
            throw new IllegalArgumentException("%s topic is not partitioned, but publication to %d partition was requested"
                    .formatted(topic.name(), publication.partition()));
            // partitions are numbered from 0
        } else if (topic.partitions() > -1 && (publication.partition() + 1) > topic.partitions()) {
            throw new IllegalArgumentException("%s topic has only %d partitions, but publishing to %d was requested"
                    .formatted(topic.name(), topic.partitions(), publication.partition()));
        }
    }

    private TopicDefinition findTopicDefinition(String name) {
        return topicDefinitionsCache.getLoadingIf(name)
                .orElseThrow(() -> new IllegalArgumentException("topic of %s name doesn't exist".formatted(name)));
    }

    @Override
    public void configurePartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
    }

    @Override
    public Partitioner partitioner() {
        return partitioner;
    }

    @Override
    public void startConsumer(String topic, String name, Consumer<Event> consumer) {
        startConsumer(topic, name, consumer, DEFAULT_POLLING_DELAY);
    }

    @Override
    public void startConsumer(String topic, String name, Consumer<Event> consumer, Duration pollingDelay) {
        startConsumer(topic, name, consumer, pollingDelay, DEFAULT_IN_MEMORY_EVENTS);
    }

    @Override
    public void startConsumer(String topic, String name, Consumer<Event> consumer,
                              Duration pollingDelay, int maxInMemoryEvents) {
        startBatchConsumer(topic, name, new ConsumerWrapper(consumer),
                new ConsumptionConfig(1, maxInMemoryEvents, pollingDelay, pollingDelay));
    }

    @Override
    public void configureDLTEventFactory(DLTEventFactory dltEventFactory) {
        this.dltEventFactory = dltEventFactory;
    }

    // This can change, trigger reload from time to time
    private List<com.binaryigor.eventsql.internal.Consumer> findPartitionedConsumers(String topic, String name) {
        var partitionedConsumers = consumerRepository.allOf(topic, name);
        if (partitionedConsumers.isEmpty()) {
            throw new IllegalArgumentException("There are no consumers of %s topic and %s name".formatted(topic, name));
        }
        return partitionedConsumers;
    }

    private void consumeEvents(ConsumerId consumerId,
                               Consumer<List<Event>> consumer,
                               ConsumptionConfig consumptionConfig) {
        var delayNextPolling = new AtomicBoolean(false);
        var lastConsumptionAt = new AtomicReference<>(clock.instant());
        while (running.get()) {
            try {
                if (delayNextPolling.get()) {
                    Thread.sleep(consumptionConfig.pollingDelay());
                }
                transactions.execute(() -> {
                    var delayNext = consumeNextEvents(consumerId, consumptionConfig, consumer, lastConsumptionAt);
                    delayNextPolling.set(delayNext);
                });
            } catch (Exception e) {
                logger.error("Problem while consuming events for {} consumer: ", consumer, e);
            }
        }
        consumerThreads.remove(consumerId);
    }

    private boolean consumeNextEvents(ConsumerId consumerId,
                                      ConsumptionConfig consumptionConfig,
                                      Consumer<List<Event>> consumer,
                                      AtomicReference<Instant> lastConsumptionAt) {
        var consumerStateOpt = consumerRepository.ofIdForUpdateSkippingLocked(consumerId);
        if (consumerStateOpt.isEmpty()) {
            return true;
        }
        var consumerState = consumerStateOpt.get();

        var events = nextEvents(consumerState, consumptionConfig.maxEvents());
        if (events.isEmpty()) {
            return true;
        }
        if (events.size() < consumptionConfig.minEvents() &&
                shouldWaitForMinEvents(lastConsumptionAt.get(), consumptionConfig.maxPollingDelay())) {
            return true;
        }

        Long firstEventId;
        long lastEventId;
        boolean delayNextPolling;
        try {
            consumer.accept(events);
            firstEventId = events.getFirst().id();
            lastEventId = events.getLast().id();
            delayNextPolling = events.size() < consumptionConfig.maxEvents();
        } catch (EventSQLConsumptionException e) {
            var dltEvent = dltEventFactory.create(e, consumerId.name());
            if (dltEvent.isPresent()) {
                logger.error("Problem while consuming event for {} consumer, publishing it to dlt: ", consumerId, e);
                lastEventId = e.event().id();
                publish(dltEvent.get());
                delayNextPolling = false;
            } else {
                logger.error("Problem while consuming event for {} consumer: ", consumerId, e);
                lastEventId = e.event().id() - 1;
                delayNextPolling = true;
            }
            firstEventId = null;
        }

        var now = clock.instant();

        var updatedConsumerState = consumerState.withUpdatedStats(firstEventId, lastEventId, now, events.size());
        consumerRepository.update(updatedConsumerState);

        lastConsumptionAt.set(now);

        return delayNextPolling;
    }

    private List<Event> nextEvents(com.binaryigor.eventsql.internal.Consumer consumer, int limit) {
        if (consumer.partition() == -1) {
            return eventRepository.nextEvents(consumer.topic(), consumer.lastEventId(), limit);
        }
        return eventRepository.nextEvents(consumer.topic(), consumer.partition(), consumer.lastEventId(), limit);
    }

    private boolean shouldWaitForMinEvents(Instant lastConsumptionAt, Duration maxPoolingDelay) {
        return Duration.between(lastConsumptionAt, clock.instant()).compareTo(maxPoolingDelay) < 0;
    }

    @Override
    public void startBatchConsumer(String topic, String name,
                                   Consumer<List<Event>> consumer,
                                   ConsumptionConfig consumptionConfig) {
        var consumers = findPartitionedConsumers(topic, name);
        for (var c : consumers) {
            var cid = new ConsumerId(c.topic(), c.name(), c.partition());
            if (consumerThreads.containsKey(cid)) {
                logger.info("Consumer {} is registered already, skipping", cid);
                break;
            }
            consumerThreads.put(cid, Thread.startVirtualThread(() -> consumeEvents(cid, consumer, consumptionConfig)));
        }
    }

    // TODO: refactor
    @Override
    public void stop(Duration timeout) {
        logger.info("Stopping publisher and consumers...");
        running.set(false);
        var latch = waitForPublisherAndConsumersToFinishAsync();
        try {
            if (latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                logger.info("Publisher and all consumers have stopped gracefully!");
            } else {
                logger.warn("Publisher or some consumers didn't finish in {}, exiting in any case", timeout);
            }
        } catch (Exception e) {
            logger.error("Problem while stopping publisher/consumers", e);
        }
    }

    private CountDownLatch waitForPublisherAndConsumersToFinishAsync() {
        var latch = new CountDownLatch(1);
        Thread.startVirtualThread(() -> {
            while (true) {
                var alivePublisher = publishThreadSet.get() && publishThread.isAlive();
                var aliveConsumers = consumerThreads.entrySet().stream()
                        .filter(e -> e.getValue().isAlive())
                        .map(Map.Entry::getKey)
                        .toList();
                if (!alivePublisher && aliveConsumers.isEmpty()) {
                    latch.countDown();
                    break;
                } else {
                    try {
                        if (alivePublisher) {
                            logger.info("Publisher is still alive, waiting for it to finish");
                        }
                        if (!aliveConsumers.isEmpty()) {
                            logger.info("Some consumers are still alive, waiting for them to finish: {}", aliveConsumers);
                        }
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        });
        return latch;
    }

    public DLTEventFactory dltEventFactory() {
        return dltEventFactory;
    }
}
