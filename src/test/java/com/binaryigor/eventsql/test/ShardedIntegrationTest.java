package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLDialect;
import com.binaryigor.eventsql.internal.EventRepository;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLConsumers;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLPublisher;
import com.binaryigor.eventsql.internal.sharded.ShardedEventSQLRegistry;
import com.binaryigor.eventsql.internal.sql.SQLEventRepository;
import com.binaryigor.eventsql.internal.sql.SQLTransactions;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.List;

import static com.binaryigor.eventsql.test.IntegrationTest.*;

public abstract class ShardedIntegrationTest {

    protected static final int SHARDS = 2;
    protected static final PostgreSQLContainer<?> POSTGRES1 = postgreSQLContainer();
    protected static final PostgreSQLContainer<?> POSTGRES2 = postgreSQLContainer();
    protected static final List<DataSource> dataSources;
    protected static final List<DSLContext> dslContexts;

    static {
        POSTGRES1.start();
        POSTGRES2.start();

        dataSources = List.of(dataSource(POSTGRES1), dataSource(POSTGRES2));
        dslContexts = dataSources.stream().map(IntegrationTest::dslContext).toList();

        dslContexts.forEach(IntegrationTest::initDbSchema);
    }

    protected EventSQL eventSQL;
    protected ShardedEventSQLRegistry registry;
    protected ShardedEventSQLPublisher publisher;
    protected ShardedEventSQLConsumers consumers;
    protected EventSQLConsumers.DLTEventFactory dltEventFactory;
    protected List<? extends EventRepository> eventRepositories;

    @BeforeEach
    protected void baseSetup() {
        var testClock = new TestClock();
        eventSQL = new EventSQL(dataSources, EventSQLDialect.POSTGRES, testClock);
        registry = (ShardedEventSQLRegistry) eventSQL.registry();
        publisher = (ShardedEventSQLPublisher) eventSQL.publisher();
        consumers = (ShardedEventSQLConsumers) eventSQL.consumers();

        dltEventFactory = eventSQL.consumers().dltEventFactory();

        var transactions = dslContexts.stream().map(SQLTransactions::new).toList();
        eventRepositories = transactions.stream().map(t -> new SQLEventRepository(t, SQLDialect.POSTGRES)).toList();

        dslContexts.forEach(ctx -> cleanDb(ctx, registry));
    }

    @AfterEach
    protected void baseTearDown() {
        consumers.stop(Duration.ofSeconds(3));
    }

}
