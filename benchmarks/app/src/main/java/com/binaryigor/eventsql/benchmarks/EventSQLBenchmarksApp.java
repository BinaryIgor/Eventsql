package com.binaryigor.eventsql.benchmarks;

import com.binaryigor.eventsql.EventSQL;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.SQLDialect;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;
import java.time.Clock;

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

    @Bean
    EventSQL eventSQL(Clock clock, EventsProperties eventsProperties) {
        var dataSources = eventsProperties.dataSources().stream()
                .map(this::dataSource)
                .toList();
        return new EventSQL(dataSources, SQLDialect.POSTGRES, clock);
    }

    private DataSource dataSource(EventsProperties.DataSourceProperties properties) {
        var config = new HikariConfig();
        config.setJdbcUrl(properties.url());
        config.setUsername(properties.username());
        config.setPassword(properties.password());
        config.setMinimumIdle(properties.connections());
        config.setMaximumPoolSize(properties.connections());
        return new HikariDataSource(config);
    }
}
