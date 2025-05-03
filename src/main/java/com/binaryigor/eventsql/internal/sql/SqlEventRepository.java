package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.internal.EventInput;
import com.binaryigor.eventsql.internal.EventRepository;
import org.jooq.Field;
import org.jooq.JSON;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class SqlEventRepository implements EventRepository {

    private static final Logger logger = LoggerFactory.getLogger(SqlEventRepository.class);
    private static final Table<?> EVENT = DSL.table("event");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<Long> ID = DSL.field("id", Long.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<String> KEY = DSL.field("key", String.class);
    private static final Field<byte[]> VALUE = DSL.field("value", byte[].class);
    private static final Field<JSON> METADATA = DSL.field("metadata", JSON.class);
    private final DslContextProvider contextProvider;
    private final SQLDialect dialect;

    public SqlEventRepository(DslContextProvider contextProvider, SQLDialect dialect) {
        this.contextProvider = contextProvider;
        this.dialect = dialect;
    }

    @Override
    public void createPartition(String topic) {
        if (dialect == SQLDialect.POSTGRES) {
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
        if (dialect == SQLDialect.POSTGRES) {
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
            var metadataJSON = SimpleJsonMapper.toJson(e.metadata());
            insert.values(e.topic(), ei.partition(), e.key(), e.value(), JSON.valueOf(metadataJSON));
        });

        insert.execute();
    }

    @Override
    public List<Event> nextEvents(String topic, Long lastId, int limit) {
        return nextEvents(topic, null, lastId, limit);
    }

    private List<Event> nextEvents(String topic, Short partition, Long lastId, int limit) {
        var condition = TOPIC.eq(topic);
        if (lastId != null) {
            condition = condition.and(ID.greaterThan(lastId));
        }
        if (partition != null) {
            condition = condition.and(PARTITION.eq(partition));
        }
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
        return nextEvents(topic, Short.valueOf((short) partition), lastId, limit);
    }
}
