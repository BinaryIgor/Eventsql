package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.DefaultEventSQLRegistry;
import com.binaryigor.eventsql.internal.EventSQLOps;
import com.binaryigor.eventsql.internal.TopicDefinitionsCache;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLConsumers;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLPublisher;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLRegistry;
import com.binaryigor.eventsql.internal.sql.SQLConsumerRepository;
import com.binaryigor.eventsql.internal.sql.SQLEventRepository;
import com.binaryigor.eventsql.internal.sql.SQLTopicRepository;
import com.binaryigor.eventsql.internal.sql.SQLTransactions;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class EventSQL {

    private static final int DEFAULT_FLUSH_PUBLISH_BUFFER_SIZE = 1000;
    private static final Duration DEFAULT_FLUSH_PUBLISH_BUFFER_DELAY = Duration.ofMillis(50);
    private final EventSQLRegistry registry;
    private final EventSQLPublisher publisher;
    private final EventSQLConsumers consumers;

    public EventSQL(DataSource dataSource, EventSQLDialect dialect) {
        this(dataSource, dialect, Clock.systemUTC());
    }

    public EventSQL(DataSource dataSource, EventSQLDialect dialect, Clock clock) {
        this(List.of(dataSource), dialect, clock);
    }

    public EventSQL(Collection<DataSource> dataSources, EventSQLDialect dialect) {
        this(dataSources, dialect, Clock.systemUTC());
    }

    public EventSQL(Collection<DataSource> dataSources, EventSQLDialect dialect, Clock clock) {
        this(dataSources, dialect, clock, DEFAULT_FLUSH_PUBLISH_BUFFER_SIZE, DEFAULT_FLUSH_PUBLISH_BUFFER_DELAY);
    }

    public EventSQL(Collection<DataSource> dataSources, EventSQLDialect dialect, Clock clock,
                    int flushPublishBufferSize, Duration flushPublishBufferDelay) {
        if (dataSources.isEmpty()) {
            throw new IllegalArgumentException("At least one data source is required");
        }

        var registryList = new ArrayList<EventSQLRegistry>();
        var publisherList = new ArrayList<EventSQLPublisher>();
        var consumersList = new ArrayList<EventSQLConsumers>();

        System.setProperty("org.jooq.no-logo", "true");
        System.setProperty("org.jooq.no-tips", "true");

        var jooqDialect = SQLDialect.valueOf(dialect.name());

        dataSources.forEach(dataSource -> {
            var dslContext = DSL.using(dataSource, jooqDialect);
            var transactions = new SQLTransactions(dslContext);

            var topicRepository = new SQLTopicRepository(transactions);
            var consumerRepository = new SQLConsumerRepository(transactions);
            var eventRepository = new SQLEventRepository(transactions, transactions, dialect);

            var registry = new DefaultEventSQLRegistry(topicRepository, eventRepository, consumerRepository, transactions);

            var topicDefinitionsCache = new TopicDefinitionsCache(topicRepository);
            var ops = new EventSQLOps(topicDefinitionsCache, transactions, consumerRepository, eventRepository, clock,
                    flushPublishBufferSize, flushPublishBufferDelay);

            registryList.add(registry);
            publisherList.add(ops);
            consumersList.add(ops);
        });

        if (dataSources.size() == 1) {
            registry = registryList.getFirst();
            publisher = publisherList.getFirst();
            consumers = consumersList.getFirst();
        } else {
            registry = new ShardedEventSQLRegistry(unmodifiableList(registryList));
            publisher = new ShardedEventSQLPublisher(unmodifiableList(publisherList));
            consumers = new ShardedEventSQLConsumers(unmodifiableList(consumersList));
        }
    }

    public EventSQLRegistry registry() {
        return registry;
    }

    public EventSQLPublisher publisher() {
        return publisher;
    }

    public EventSQLConsumers consumers() {
        return consumers;
    }
}
