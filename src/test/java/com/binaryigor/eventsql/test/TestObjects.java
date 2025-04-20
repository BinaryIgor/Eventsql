package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventPublication;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class TestObjects {

    private static final Random RANDOM = new Random();

    public static EventPublication randomEventPublication(String topic, int partition) {
        return randomEventPublication(topic, RANDOM.nextBoolean() ? null : UUID.randomUUID().toString(), partition);
    }

    public static EventPublication randomEventPublication(String topic, String key, int partition) {
        var value = new byte[8];
        RANDOM.nextBytes(value);

        Map<String, String> metadata = RANDOM.nextBoolean() ? Map.of() : Map.of("consumer", UUID.randomUUID().toString());

        return new EventPublication(topic, partition, key, value, metadata);
    }

    public static EventPublication randomEventPublication(String topic, String key) {
        return randomEventPublication(topic, key, -1);
    }

    public static EventPublication randomEventPublication(String topic) {
        return randomEventPublication(topic, -1);
    }

    public static int randomInt(int min, int max) {
        return max + RANDOM.nextInt(max - min);
    }
}
