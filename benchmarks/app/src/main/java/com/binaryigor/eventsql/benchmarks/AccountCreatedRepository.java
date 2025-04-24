package com.binaryigor.eventsql.benchmarks;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

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
}
