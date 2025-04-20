package com.binaryigor.eventsql;

import com.binaryigor.eventsql.impl.EventSQLOps;
import com.binaryigor.eventsql.impl.EventSQLRegistryImpl;
import com.binaryigor.eventsql.impl.TopicDefinitionsCache;
import com.binaryigor.eventsql.impl.sql.SqlConsumerRepository;
import com.binaryigor.eventsql.impl.sql.SqlEventRepository;
import com.binaryigor.eventsql.impl.sql.SqlTopicRepository;
import com.binaryigor.eventsql.impl.sql.SqlTransactions;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.time.Clock;
import java.util.Optional;

public class EventSQL {

    private final EventSQLRegistry registry;
    private final EventSQLOps ops;

    public EventSQL(DataSource dataSource, SQLDialect sqlDialect, Clock clock) {
        this(dataSource, sqlDialect, clock, Optional.empty());
    }

    public EventSQL(DataSource dataSource,
                    SQLDialect sqlDialect,
                    Clock clock,
                    Optional<EventSQLConsumers.DLTEventFactory> dltEventFactory) {
        var dslContext = DSL.using(dataSource, sqlDialect);
        var transactions = new SqlTransactions(dslContext);

        var topicRepository = new SqlTopicRepository(transactions);
        var consumerRepository = new SqlConsumerRepository(transactions);
        var eventRepository = new SqlEventRepository(transactions);

        registry = new EventSQLRegistryImpl(topicRepository, eventRepository, consumerRepository, transactions);

        var topicDefinitionsCache = new TopicDefinitionsCache(topicRepository);
        ops = new EventSQLOps(topicDefinitionsCache, transactions, consumerRepository, eventRepository, clock);
        dltEventFactory.ifPresent(ops::configureDLTEventFactory);
    }

    public EventSQLRegistry registry() {
        return registry;
    }

    public EventSQLPublisher publisher() {
        return ops;
    }

    public EventSQLConsumers consumers() {
        return ops;
    }

    public EventSQLConsumers.DLTEventFactory configuredDltEventFactory() {
        return ops.dltEventFactory();
    }
}
