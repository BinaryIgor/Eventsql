package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.EventPublication;
import com.binaryigor.eventsql.EventSQLPublisher;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Random;

public class DefaultPartitioner implements EventSQLPublisher.Partitioner {

    private static final Random RANDOM = new Random();

    @Override
    public int partition(EventPublication publication, int topicPartitions) {
        if (topicPartitions == -1) {
            return -1;
        }
        if (publication.key() == null) {
            return RANDOM.nextInt(topicPartitions);
        }

        return keyHash(publication.key())
                .mod(BigInteger.valueOf(topicPartitions))
                .intValue();
    }

    private BigInteger keyHash(String key) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(key.getBytes(StandardCharsets.UTF_8));
            return new BigInteger(1, hashBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
