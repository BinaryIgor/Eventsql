package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.internal.Consumer;
import com.binaryigor.eventsql.internal.ConsumerId;
import com.binaryigor.eventsql.internal.ConsumerRepository;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class SQLConsumerRepository implements ConsumerRepository {

    private static final Table<?> CONSUMER = DSL.table("consumer");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<String> NAME = DSL.field("name", String.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<Long> LAST_EVENT_ID = DSL.field("last_event_id", Long.class);
    private static final Field<Instant> LAST_CONSUMPTION_AT = DSL.field("last_consumption_at", Instant.class);
    private final DSLContextProvider contextProvider;

    public SQLConsumerRepository(DSLContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    @Override
    public void save(Consumer consumer) {
        saveAll(List.of(consumer));
    }

    @Override
    public void saveAll(Collection<Consumer> consumers) {
        if (consumers.isEmpty()) {
            return;
        }
        var insert = contextProvider.get()
                .insertInto(CONSUMER)
                .columns(TOPIC, NAME, PARTITION, LAST_EVENT_ID, LAST_CONSUMPTION_AT);

        consumers.forEach(c -> insert.values(c.topic(), c.name(), (short) c.partition(), c.lastEventId(), c.lastConsumptionAt()));

        insert.onConflict(TOPIC, NAME, PARTITION)
                .doUpdate()
                .set(LAST_EVENT_ID, DSL.excluded(LAST_EVENT_ID))
                .set(LAST_CONSUMPTION_AT, DSL.excluded(LAST_CONSUMPTION_AT))
                .execute();
    }

    @Override
    public List<Consumer> all() {
        return allOf(DSL.trueCondition());
    }

    private List<Consumer> allOf(Condition condition) {
        return contextProvider.get()
                .select(TOPIC, NAME, PARTITION, LAST_EVENT_ID, LAST_CONSUMPTION_AT)
                .from(CONSUMER)
                .where(condition)
                .fetchInto(Consumer.class);
    }

    @Override
    public List<Consumer> allOf(String topic, String name) {
        return allOf(TOPIC.eq(topic).and(NAME.eq(name)));
    }

    @Override
    public List<Consumer> allOf(String topic) {
        return allOf(TOPIC.eq(topic));
    }

    @Override
    public Optional<Consumer> ofIdForUpdateSkippingLocked(ConsumerId id) {
        return contextProvider.get()
                .select(TOPIC, NAME, PARTITION, LAST_EVENT_ID, LAST_CONSUMPTION_AT)
                .from(CONSUMER)
                .where(TOPIC.eq(id.topic())
                        .and(NAME.eq(id.name()))
                        .and(PARTITION.eq((short) id.partition())))
                .forUpdate()
                .skipLocked()
                .fetchOptionalInto(Consumer.class);
    }

    @Override
    public void update(ConsumerId id, long lastEventId, Instant lastConsumptionAt) {
        contextProvider.get()
                .update(CONSUMER)
                .set(LAST_EVENT_ID, lastEventId)
                .set(LAST_CONSUMPTION_AT, lastConsumptionAt)
                .where(TOPIC.eq(id.topic())
                        .and(NAME.eq(id.name()))
                        .and(PARTITION.eq((short) id.partition())))
                .execute();
    }

    @Override
    public void deleteAllOf(String topic, String name) {
        contextProvider.get()
                .delete(CONSUMER)
                .where(TOPIC.eq(topic).and(NAME.eq(name)))
                .execute();
    }
}
