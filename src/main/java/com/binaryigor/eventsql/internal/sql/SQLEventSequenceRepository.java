package com.binaryigor.eventsql.internal.sql;

import com.binaryigor.eventsql.internal.EventSequence;
import com.binaryigor.eventsql.internal.EventSequenceKey;
import com.binaryigor.eventsql.internal.EventSequenceRepository;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.Collection;
import java.util.Optional;

public class SQLEventSequenceRepository implements EventSequenceRepository {

    private static final Table<?> EVENT_SEQUENCE = DSL.table("event_sequence");
    private static final Field<String> TOPIC = DSL.field("topic", String.class);
    private static final Field<Short> PARTITION = DSL.field("partition", Short.class);
    private static final Field<Long> NEXT_SEQ = DSL.field("next_seq", Long.class);
    private final DSLContextProvider contextProvider;

    public SQLEventSequenceRepository(DSLContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    @Override
    public void saveAll(Collection<EventSequence> eventSequences) {
        if (eventSequences.isEmpty()) {
            return;
        }

        var insert = contextProvider.get()
                .insertInto(EVENT_SEQUENCE)
                .columns(TOPIC, PARTITION, NEXT_SEQ);

        eventSequences.forEach(es -> {
            insert.values(es.topic(), (short) es.partition(), es.nextSeq());
        });

        insert.onConflict(TOPIC, PARTITION)
                .doUpdate()
                .set(NEXT_SEQ, DSL.excluded(NEXT_SEQ))
                .execute();
    }

    @Override
    public Optional<EventSequence> ofKeyForUpdate(EventSequenceKey key) {
        return contextProvider.get()
                .select(TOPIC, PARTITION, NEXT_SEQ)
                .from(EVENT_SEQUENCE)
                .where(TOPIC.eq(key.topic()).and(PARTITION.eq(key.partition())))
                .forUpdate()
                .fetchOptionalInto(EventSequence.class);
    }

    @Override
    public void deleteAllOfTopic(String topic) {
        contextProvider.get()
                .deleteFrom(EVENT_SEQUENCE)
                .where(TOPIC.eq(topic))
                .execute();
    }
}
