package com.binaryigor.eventsql;

import com.binaryigor.eventsql.internal.EventSQLOps;
import com.binaryigor.eventsql.internal.DefaultEventSQLRegistry;
import com.binaryigor.eventsql.internal.TopicDefinitionsCache;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLConsumers;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLPublisher;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLRegistry;
import com.binaryigor.eventsql.internal.sql.SqlConsumerRepository;
import com.binaryigor.eventsql.internal.sql.SqlEventRepository;
import com.binaryigor.eventsql.internal.sql.SqlTopicRepository;
import com.binaryigor.eventsql.internal.sql.SqlTransactions;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class EventSQL {

    private final EventSQLRegistry registry;
    private final EventSQLPublisher publisher;
    private final EventSQLConsumers consumers;

    public EventSQL(DataSource dataSource, SQLDialect sqlDialect) {
        this(dataSource, sqlDialect, Clock.systemUTC(), Optional.empty());
    }

    public EventSQL(DataSource dataSource, SQLDialect sqlDialect, Clock clock) {
        this(dataSource, sqlDialect, clock, Optional.empty());
    }

    public EventSQL(DataSource dataSource,
                    SQLDialect sqlDialect,
                    Clock clock,
                    Optional<EventSQLConsumers.DLTEventFactory> dltEventFactory) {
        this(List.of(dataSource), sqlDialect, clock, dltEventFactory);
    }

    public EventSQL(Collection<DataSource> dataSources, SQLDialect sqlDialect) {
        this(dataSources, sqlDialect, Clock.systemUTC(), Optional.empty());
    }

    public EventSQL(Collection<DataSource> dataSources,
                    SQLDialect sqlDialect,
                    Clock clock) {
        this(dataSources, sqlDialect, clock, Optional.empty());
    }

    public EventSQL(Collection<DataSource> dataSources,
                    SQLDialect sqlDialect,
                    Clock clock,
                    Optional<EventSQLConsumers.DLTEventFactory> dltEventFactory) {
        if (dataSources.isEmpty()) {
            throw new IllegalArgumentException("At least one data source is required");
        }

        var registryList = new ArrayList<EventSQLRegistry>();
        var publisherList = new ArrayList<EventSQLPublisher>();
        var consumersList = new ArrayList<EventSQLConsumers>();

        System.setProperty("org.jooq.no-logo", "true");
        System.setProperty("org.jooq.no-tips", "true");

        dataSources.forEach(dataSource -> {
            var dslContext = DSL.using(dataSource, sqlDialect);
            var transactions = new SqlTransactions(dslContext);

            var topicRepository = new SqlTopicRepository(transactions);
            var consumerRepository = new SqlConsumerRepository(transactions);
            var eventRepository = new SqlEventRepository(transactions);

            var registry = new DefaultEventSQLRegistry(topicRepository, eventRepository, consumerRepository, transactions);

            var topicDefinitionsCache = new TopicDefinitionsCache(topicRepository);
            var ops = new EventSQLOps(topicDefinitionsCache, transactions, consumerRepository, eventRepository, clock);
            dltEventFactory.ifPresent(ops::configureDLTEventFactory);

            registryList.add(registry);
            publisherList.add(ops);
            consumersList.add(ops);
        });


        if (dataSources.size() == 1) {
            registry = registryList.getFirst();
            publisher = publisherList.getFirst();
            consumers = consumersList.getFirst();
        } else {
            registry = new ShardedEventSQLRegistry(registryList);
            publisher = new ShardedEventSQLPublisher(publisherList);
            consumers = new ShardedEventSQLConsumers(consumersList);
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
