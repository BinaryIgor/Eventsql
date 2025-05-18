package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.internal.EventInput;
import com.binaryigor.eventsql.internal.EventRepository;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.DayToSecond;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

public class SQLEventRepository implements EventRepository {

    private static final Logger logger = LoggerFactory.getLogger(SQLEventRepository.class);
    private static final Table<?> EVENT = DSL.table("event");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<Long> ID = DSL.field("id", Long.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<String> KEY = DSL.field("key", String.class);
    private static final Field<byte[]> VALUE = DSL.field("value", byte[].class);
    private static final Field<JSON> METADATA = DSL.field("metadata", JSON.class);
    private static final Field<Timestamp> CREATED_AT = DSL.field("created_at", Timestamp.class);
    private final DSLContextProvider contextProvider;
    private final EventSQL.Dialect dialect;
    private final int nextEventsReadVisibilityThreshold;

    public SQLEventRepository(DSLContextProvider contextProvider, EventSQL.Dialect dialect, int nextEventsReadVisibilityThreshold) {
        this.contextProvider = contextProvider;
        this.dialect = dialect;
        this.nextEventsReadVisibilityThreshold = nextEventsReadVisibilityThreshold;
    }

    @Override
    public void createPartition(String topic) {
        if (dialect == EventSQL.Dialect.POSTGRES) {
            contextProvider.get()
                    .execute("CREATE TABLE %s PARTITION OF %s FOR VALUES IN ('%s')"
                            .formatted(topicTablePartitionName(topic), EVENT.getName(), topic));
        } else {
            warnPartitionManagementNotSupportedForDialect();
        }
    }

    private void warnPartitionManagementNotSupportedForDialect() {
        logger.warn("Partition management is not supported for {} SQL dialect. Manage it manually or consider opening a Pull Request :)", dialect);
    }

    private String topicTablePartitionName(String topic) {
        return "%s_%s".formatted(topic, EVENT.getName());
    }

    @Override
    public void deletePartition(String topic) {
        if (dialect == EventSQL.Dialect.POSTGRES) {
            contextProvider.get()
                    .dropTableIfExists(topicTablePartitionName(topic))
                    .cascade()
                    .execute();
        } else {
            warnPartitionManagementNotSupportedForDialect();
        }
    }

    @Override
    public void create(EventInput event) {
        createAll(List.of(event));
    }

    @Override
    public void createAll(Collection<EventInput> events) {
        if (events.isEmpty()) {
            return;
        }

        var insert = contextProvider.get()
                .insertInto(EVENT)
                .columns(TOPIC, PARTITION, KEY, VALUE, METADATA);

        events.forEach(ei -> {
            var e = ei.publication();
            var metadataJSON = SimpleJSONMapper.toJSON(e.metadata());
            insert.values(e.topic(), ei.partition(), e.key(), e.value(), JSON.valueOf(metadataJSON));
        });

        insert.execute();
    }

    @Override
    public List<Event> nextEvents(String topic, Long lastId, int limit) {
        return nextEvents(topic, null, lastId, nextEventsReadVisibilityThreshold, limit);
    }

    public List<Event> nextEvents(String topic, Short partition, Long lastId,
                                  int visibilityThreshold,
                                  int limit) {
        var condition = TOPIC.eq(topic);
        if (lastId != null) {
            condition = condition.and(ID.greaterThan(lastId));
        }
        if (partition != null) {
            condition = condition.and(PARTITION.eq(partition));
        }

        var createdAtThreshold = DSL.currentTimestamp().minus(DayToSecond.valueOf(Duration.ofMillis(visibilityThreshold)));
        condition = condition.and(CREATED_AT.lessThan(createdAtThreshold));

        return contextProvider.get()
                .select(TOPIC, ID, PARTITION, KEY, VALUE, METADATA)
                .from(EVENT)
                .where(condition)
                .orderBy(ID)
                .limit(limit)
                .fetchInto(Event.class);
    }

    @Override
    public List<Event> nextEvents(String topic, int partition, Long lastId, int limit) {
        return nextEvents(topic, (short) partition, lastId, nextEventsReadVisibilityThreshold, limit);
    }
}
