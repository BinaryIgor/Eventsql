package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.EventSQL;
import com.binaryigor.eventsql.EventSQLConsumers;
import com.binaryigor.eventsql.EventSQLPublisher;
import org.jooq.SQLDialect;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.time.Clock;
import java.util.List;

@SpringBootApplication
@EnableConfigurationProperties(EventsProperties.class)
public class EventSQLBenchmarksApp {
    public static void main(String[] args) {
        SpringApplication.run(EventSQLBenchmarksApp.class, args);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Primary
    @Bean
    @ConfigurationProperties("spring.datasource")
    DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("events.datasource1")
    DataSourceProperties eventSQLDataSourceProperties1() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("events.datasource2")
    DataSourceProperties eventSQLDataSourceProperties2() {
        return new DataSourceProperties();
    }

    @Bean
    @ConfigurationProperties("events.datasource3")
    DataSourceProperties eventSQLDataSourceProperties3() {
        return new DataSourceProperties();
    }

    @Bean
    EventSQL eventSQL(Clock clock) {
        var dataSource1 = eventSQLDataSourceProperties1()
                .initializeDataSourceBuilder()
                .build();
        var dataSource2 = eventSQLDataSourceProperties2()
                .initializeDataSourceBuilder()
                .build();
        var dataSource3 = eventSQLDataSourceProperties3()
                .initializeDataSourceBuilder()
                .build();
        return EventSQL.sharded(List.of(dataSource1, dataSource2, dataSource3), SQLDialect.POSTGRES, clock);
    }

    @Bean
    EventSQLConsumers eventSQLConsumers(EventSQL eventSQL) {
        return eventSQL.consumers();
    }

    @Bean
    EventSQLPublisher eventSQLPublisher(EventSQL eventSQL) {
        return eventSQL.publisher();
    }
}
