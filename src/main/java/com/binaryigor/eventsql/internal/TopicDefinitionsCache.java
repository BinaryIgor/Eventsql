package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.TopicDefinition;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

// TODO: reload smarter
public class TopicDefinitionsCache {

    private final Map<String, Optional<TopicDefinition>> cache = new ConcurrentHashMap<>();
    private final TopicRepository topicRepository;

    public TopicDefinitionsCache(TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    public Optional<TopicDefinition> getLoadingIf(String name) {
        return getLoadingIf(name, false);
    }

    public Optional<TopicDefinition> getLoadingIf(String name, boolean cacheEmpty) {
        return cache.getOrDefault(name, Optional.empty())
                .or(() -> {
                    if (cacheEmpty && cache.containsKey(name)) {
                        return Optional.empty();
                    }
                    var tDefOpt = topicRepository.ofName(name);
                    tDefOpt.ifPresentOrElse(tDef -> cache.put(tDef.name(), Optional.of(tDef)),
                            () -> {
                                if (cacheEmpty) {
                                    cache.put(name, Optional.empty());
                                }
                            });
                    return tDefOpt;
                });
    }
}
