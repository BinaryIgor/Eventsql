package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLPublisher;
import com.binaryigor.eventsql.EventSQLRegistry;
import com.binaryigor.eventsql.internal.ConsumerRepository;
import com.binaryigor.eventsql.internal.EventRepository;
import com.binaryigor.eventsql.internal.sql.SqlConsumerRepository;
import com.binaryigor.eventsql.internal.sql.SqlEventRepository;
import com.binaryigor.eventsql.internal.sql.SqlTransactions;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Duration;

public abstract class IntegrationTest {

    protected static final PostgreSQLContainer<?> POSTGRES = postgreSQLContainer();
    protected static final DataSource dataSource;
    protected static final DSLContext dslContext;

    static {
        POSTGRES.start();
        dataSource = dataSource(POSTGRES);
        dslContext = dslContext(dataSource);
        initDbSchema(dslContext);
    }

    static PostgreSQLContainer<?> postgreSQLContainer() {
        return new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
    }

    static DataSource dataSource(PostgreSQLContainer<?> postgres) {
        var config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        return new HikariDataSource(config);
    }

    static DSLContext dslContext(DataSource dataSource) {
        return DSL.using(dataSource, SQLDialect.POSTGRES);
    }

    static void initDbSchema(DSLContext dslContext) {
        dslContext.execute("""
                CREATE TABLE topic (
                    name TEXT PRIMARY KEY,
                    partitions SMALLINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
                
                CREATE TABLE consumer (
                    topic TEXT NOT NULL,
                    name TEXT NOT NULL,
                    partition SMALLINT NOT NULL,
                    last_event_id BIGINT,
                    last_consumption_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (topic, name, partition)
                );
                """);
    }

    static void cleanDb(DSLContext dslContext, EventSQLRegistry registry) {
        registry.listConsumers().forEach(c -> registry.unregisterConsumer(c.topic(), c.name()));
        registry.listTopics().forEach(t -> registry.unregisterTopic(t.name()));

        // hard to clear/drop all partitions, so let's just recreate the table each time
        dslContext.execute("""
                 DROP TABLE IF EXISTS event;
                 CREATE TABLE event (
                    topic TEXT NOT NULL,
                    id BIGSERIAL NOT NULL,
                    partition SMALLINT NOT NULL,
                    key TEXT,
                    value BYTEA NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    metadata JSONB NOT NULL,
                    PRIMARY KEY (topic, id)
                ) PARTITION BY LIST(topic);
                """);
    }

    protected TestClock testClock;
    protected EventSQL eventSQL;
    protected EventSQLRegistry registry;
    protected EventSQLPublisher publisher;
    protected EventSQLConsumers consumers;
    protected EventSQLConsumers.DLTEventFactory dltEventFactory;
    protected EventRepository eventRepository;
    protected ConsumerRepository consumerRepository;

    @BeforeEach
    protected void baseSetup() {
        testClock = new TestClock();
        eventSQL = newEventSQLInstance();
        registry = eventSQL.registry();
        publisher = eventSQL.publisher();
        consumers = eventSQL.consumers();

        dltEventFactory = eventSQL.consumers().dltEventFactory();

        var transactions = new SqlTransactions(dslContext);
        eventRepository = new SqlEventRepository(transactions);
        consumerRepository = new SqlConsumerRepository(transactions);

        cleanDb(dslContext, registry);
    }

    protected EventSQL newEventSQLInstance() {
        return new EventSQL(dataSource, SQLDialect.POSTGRES, testClock);
    }

    @AfterEach
    protected void baseTearDown() {
        consumers.stop(Duration.ofSeconds(3));
    }
}
