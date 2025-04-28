package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLPublisher;
import com.binaryigor.eventsql.EventSQLRegistry;
import com.binaryigor.eventsql.impl.ConsumerRepository;
import com.binaryigor.eventsql.impl.EventRepository;
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
import java.util.List;

// TODO: merge into one with IntegrationTest, configurable?
public abstract class ShardedIntegrationTest {

    protected static final PostgreSQLContainer<?> POSTGRES1 = new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
    protected static final PostgreSQLContainer<?> POSTGRES2 = new PostgreSQLContainer(DockerImageName.parse("postgres:16"));
    protected static final List<DataSource> dataSources;
    protected static final List<DSLContext> dslContexts;

    static {
        POSTGRES1.start();
        POSTGRES2.start();

        dataSources = List.of(dataSource(POSTGRES1), dataSource(POSTGRES2));
        dslContexts = dataSources.stream().map(ds -> DSL.using(ds, SQLDialect.POSTGRES)).toList();

        dslContexts.forEach(ctx -> {
            ctx.execute("""
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
        });
    }

    private TestClock testClock;
    protected EventSQL eventSQL;
    protected EventSQLRegistry registry;
    protected EventSQLPublisher publisher;
    protected EventSQLConsumers consumers;
    protected EventSQLConsumers.DLTEventFactory dltEventFactory;
    protected EventRepository eventRepository;
    protected ConsumerRepository consumerRepository;

    private static DataSource dataSource(PostgreSQLContainer<?> postgres) {
        var config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        return new HikariDataSource(config);
    }

    @BeforeEach
    protected void baseSetup() {
        dslContexts.forEach(c -> c.execute("""
                TRUNCATE topic;
                TRUNCATE consumer;
                """));

        // hard to clear/drop all partitions, so let's just recreate the table each time
        dslContexts.forEach(c -> c.execute("""
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
                """));

        testClock = new TestClock();
        eventSQL = EventSQL.sharded(dataSources, SQLDialect.POSTGRES, testClock);
        registry = eventSQL.registry();
        publisher = eventSQL.publisher();
        consumers = eventSQL.consumers();

        dltEventFactory = eventSQL.consumers().dltEventFactory();
    }

    @AfterEach
    protected void baseTearDown() {
        consumers.stop(Duration.ofSeconds(3));
    }

}
