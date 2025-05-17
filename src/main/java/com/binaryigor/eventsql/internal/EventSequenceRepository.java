package com.binaryigor.eventsql.internal;

import java.util.Collection;
import java.util.Optional;

public interface EventSequenceRepository {

    void saveAll(Collection<EventSequence> eventSequences);

    Optional<EventSequence> ofKeyForUpdate(EventSequenceKey key);

    void deleteAllOfTopic(String topic);
}
