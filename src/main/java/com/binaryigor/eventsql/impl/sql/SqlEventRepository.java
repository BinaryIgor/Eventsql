package com.binaryigor.eventsql.impl.sql;

import com.binaryigor.eventsql.Event;
import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.impl.EventRepository;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.Collection;
import java.util.List;

public class SqlEventRepository implements EventRepository {

    private static final Table<?> EVENT = DSL.table("event");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<Long> ID = DSL.field("id", Long.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<String> KEY = DSL.field("key", String.class);
    private static final Field<byte[]> VALUE = DSL.field("value", byte[].class);
    private static final Field<JSONB> METADATA = DSL.field("metadata", JSONB.class);
    private final DslContextProvider contextProvider;

    public SqlEventRepository(DslContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    @Override
    public void createPartition(String topic) {
        contextProvider.get()
                .execute("CREATE TABLE %s PARTITION OF %s FOR VALUES IN ('%s')"
                        .formatted(topicTablePartitionName(topic), EVENT.getName(), topic));
    }

    private String topicTablePartitionName(String topic) {
        return "%s_%s".formatted(topic, EVENT.getName());
    }

    @Override
    public void deletePartition(String topic) {
        contextProvider.get()
                .dropTableIfExists(topicTablePartitionName(topic))
                .cascade()
                .execute();
    }

    @Override
    public void create(EventPublication event) {
        createAll(List.of(event));
    }

    @Override
    public void createAll(Collection<EventPublication> events) {
        if (events.isEmpty()) {
            return;
        }

        var insert = contextProvider.get()
                .insertInto(EVENT)
                .columns(TOPIC, PARTITION, KEY, VALUE, METADATA);

        events.forEach(e -> {
            var metadataJSON = JSONB.valueOf(SimpleJsonMapper.toJson(e.metadata()));
            insert.values(e.topic(), (short) e.partition(), e.key(), e.value(), metadataJSON);
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
