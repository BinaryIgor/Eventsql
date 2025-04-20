package com.binaryigor.eventsql.impl.sql;

import java.util.Map;
import java.util.stream.Collectors;

public class SimpleJsonMapper {

    public static String toJson(Map<String, String> map) {
        return map.entrySet().stream()
                .map(e -> "  \"%s\": \"%s\"".formatted(e.getKey(), e.getValue()))
                .collect(Collectors.joining(",\n", "{\n", "\n}"));
    }
}
