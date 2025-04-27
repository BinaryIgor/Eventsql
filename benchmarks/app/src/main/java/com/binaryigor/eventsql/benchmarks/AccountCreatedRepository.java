package com.binaryigor.eventsql.benchmarks;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Repository
public class AccountCreatedRepository {

    private final JdbcClient jdbcClient;

    public AccountCreatedRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void save(AccountCreated accountCreated) {
        jdbcClient.sql("""
                        INSERT INTO account (id, email, name)
                        VALUES (:id, :email, :name)
                        ON CONFLICT (id)
                        DO UPDATE
                        SET name = EXCLUDED.name
                        """)
                .param("id", accountCreated.id())
                .param("email", accountCreated.email())
                .param("name", accountCreated.name())
                .update();
    }

    public void save(Collection<AccountCreated> accountsCreated) {
        if (accountsCreated.isEmpty()) {
            return;
        }

        var paramsString = accountsCreated.stream().map($ -> "(?, ?, ?)")
                        .collect(Collectors.joining(", \n"));
        var params = accountsCreated.stream().flatMap(e -> Stream.of(e.id(), e.email(), e.name()))
                        .toList();

        jdbcClient.sql("""
                        INSERT INTO account (id, email, name)
                        VALUES %s
                        ON CONFLICT (id)
                        DO UPDATE
                        SET name = EXCLUDED.name
                        """.formatted(paramsString))
                .params(params)
                .update();
    }
}
