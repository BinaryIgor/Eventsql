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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class EventSQL {

    public static final int PUBLISH_TIMEOUT = 250;
    public static final int NEXT_EVENTS_READ_VISIBILITY_THRESHOLD = 333;
    private final EventSQLRegistry registry;
    private final EventSQLPublisher publisher;
    private final EventSQLConsumers consumers;

    public static EventSQL of(DataSourceProperties dataSourceProperties) {
        return of(dataSourceProperties, Clock.systemUTC());
    }

    public static EventSQL of(DataSourceProperties dataSourceProperties, Clock clock) {
        return of(List.of(dataSourceProperties), clock);
    }

    public static EventSQL of(Collection<DataSourceProperties> dataSourceProperties) {
        return of(dataSourceProperties, Clock.systemUTC());
    }

    public static EventSQL of(Collection<DataSourceProperties> dataSourceProperties, Clock clock) {
        return new EventSQL(dataSourceProperties.stream().map(props ->
                        new DataSource(props.dialect(), dataSource(props)))
                .toList(), clock);
    }

    /**
     * Primary constructor used mainly for internal purposes.
     * It should rarely be used outside library implementation, only if static of factories do not provide what is necessary.
     * Additionally, if used, PUBLISH_TIMEOUT must be set as a query/statement timeout on each newly open data source connection.
     * If not, there is a risk of loosing some events by consumers - it is being set by static of methods when dataSource() factory is called; check them out for guidance.
     *
     * @param dataSources list of data sources where events are hosted
     * @param clock       clock used mainly for consumer delays
     */
    public EventSQL(Collection<DataSource> dataSources, Clock clock) {
        if (dataSources.isEmpty()) {
            throw new IllegalArgumentException("At least one data source is required");
        }

        var registryList = new ArrayList<EventSQLRegistry>();
        var publisherList = new ArrayList<EventSQLPublisher>();
        var consumersList = new ArrayList<EventSQLConsumers>();

        System.setProperty("org.jooq.no-logo", "true");
        System.setProperty("org.jooq.no-tips", "true");

        dataSources.forEach(dataSource -> {
            var jooqDialect = SQLDialect.valueOf(dataSource.dialect().name());
            var dslContext = DSL.using(dataSource.dataSource(), jooqDialect);
            var transactions = new SQLTransactions(dslContext);

            var topicRepository = new SQLTopicRepository(transactions);
            var consumerRepository = new SQLConsumerRepository(transactions);
            var eventRepository = new SQLEventRepository(transactions, dataSource.dialect(), NEXT_EVENTS_READ_VISIBILITY_THRESHOLD);

            var registry = new DefaultEventSQLRegistry(topicRepository, eventRepository, consumerRepository, transactions);

            var topicDefinitionsCache = new TopicDefinitionsCache(topicRepository);
            var ops = new EventSQLOps(topicDefinitionsCache, transactions, consumerRepository, eventRepository, clock);

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

    private static javax.sql.DataSource dataSource(DataSourceProperties dataSourceProperties) {
        var config = new HikariConfig();
        config.setJdbcUrl(dataSourceProperties.url());
        config.setUsername(dataSourceProperties.username());
        config.setPassword(dataSourceProperties.password());
        config.setMinimumIdle(dataSourceProperties.minPoolSize());
        config.setMaximumPoolSize(dataSourceProperties.maxPoolSize());

        switch (dataSourceProperties.dialect()) {
            case POSTGRES -> config.setConnectionInitSql("SET statement_timeout=" + PUBLISH_TIMEOUT);
            case MYSQL -> config.setConnectionInitSql("SET SESSION max_execution_time=" + PUBLISH_TIMEOUT);
            case MARIADB -> config.setConnectionInitSql("SET SESSION max_statement_time=" + (PUBLISH_TIMEOUT / 1000.0));
        }

        return new HikariDataSource(config);
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

    public enum Dialect {
        POSTGRES, MYSQL, MARIADB
    }

    public record DataSourceProperties(Dialect dialect,
                                       String url,
                                       String username,
                                       String password,
                                       int minPoolSize,
                                       int maxPoolSize) {

        public DataSourceProperties(Dialect dialect,
                                    String url,
                                    String username,
                                    String password,
                                    int poolSize) {
            this(dialect, url, username, password, poolSize, poolSize);
        }

        public DataSourceProperties(Dialect dialect,
                                    String url,
                                    String username,
                                    String password) {
            this(dialect, url, username, password, 10, 20);
        }
    }

    public record DataSource(Dialect dialect, javax.sql.DataSource dataSource) {

    }
}
