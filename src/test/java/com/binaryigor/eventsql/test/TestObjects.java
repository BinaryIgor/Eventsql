package com.binaryigor.eventsql.test;

import com.binaryigor.eventsql.EventPublication;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class TestObjects {

    private static final Random RANDOM = new Random();

    public static EventPublication randomEventPublication(String topic) {
        return randomEventPublication(topic, RANDOM.nextBoolean() ? null : UUID.randomUUID().toString());
    }

    public static EventPublication randomEventPublication(String topic, String key) {
        var value = new byte[8];
        RANDOM.nextBytes(value);

        Map<String, String> metadata = RANDOM.nextBoolean() ? Map.of() : Map.of("consumer", UUID.randomUUID().toString());

        return new EventPublication(topic, key, value, metadata);
    }

    public static int randomInt(int min, int max) {
        return max + RANDOM.nextInt(max - min);
    }
}
