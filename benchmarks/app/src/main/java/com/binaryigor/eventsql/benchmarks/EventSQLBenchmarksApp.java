package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.EventSQL;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@EnableConfigurationProperties(EventsProperties.class)
public class EventSQLBenchmarksApp {

    public static void main(String[] args) {
        SpringApplication.run(EventSQLBenchmarksApp.class, args);
    }

    @Bean
    EventSQL eventSQL(EventsProperties eventsProperties) {
        var dataSourceProperties = eventsProperties.dataSources().stream()
                .filter(EventsProperties.DataSourceProperties::enabled)
                .map(ps -> new EventSQL.DataSourceProperties(EventSQL.Dialect.POSTGRES,
                        ps.url(), ps.username(), ps.password()))
                .toList();
        return EventSQL.of(dataSourceProperties);
    }
}
