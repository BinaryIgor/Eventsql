package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLPublisher;
import com.binaryigor.eventsql.EventSQLRegistry;
import com.binaryigor.eventsql.impl.ConsumerRepository;
import com.binaryigor.eventsql.impl.EventRepository;
import com.binaryigor.eventsql.impl.sql.SqlConsumerRepository;
import com.binaryigor.eventsql.impl.sql.SqlEventRepository;
import com.binaryigor.eventsql.impl.sql.SqlTransactions;
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

    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
    protected static final DataSource dataSource;
    protected static final DSLContext dslContext;

    static {
        POSTGRES.start();

        var config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        dataSource = new HikariDataSource(config);

        dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);

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

    private TestClock testClock;
    protected EventSQL eventSQL;
    protected EventSQLRegistry registry;
    protected EventSQLPublisher publisher;
    protected EventSQLConsumers consumers;
    protected EventSQLConsumers.DLTEventFactory dltEventFactory;
    protected EventRepository eventRepository;
    protected ConsumerRepository consumerRepository;

    @BeforeEach
    protected void baseSetup() {
        dslContext.execute("""
                TRUNCATE topic;
                TRUNCATE consumer;
                """);

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

        testClock = new TestClock();
        eventSQL = new EventSQL(dataSource, SQLDialect.POSTGRES, testClock);
        registry = eventSQL.registry();
        publisher = eventSQL.publisher();
        consumers = eventSQL.consumers();

        dltEventFactory = eventSQL.configuredDltEventFactory();

        var transactions = new SqlTransactions(dslContext);
        eventRepository = new SqlEventRepository(transactions);
        consumerRepository = new SqlConsumerRepository(transactions);
    }

    @AfterEach
    protected void baseTearDown() {
        consumers.stop(Duration.ofSeconds(3));
    }

}
