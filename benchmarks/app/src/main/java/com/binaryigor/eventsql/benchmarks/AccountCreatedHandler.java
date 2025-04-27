package com.binaryigor.eventsql.benchmarks;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class AccountCreatedHandler {

    private final AccountCreatedRepository accountCreatedRepository;
    private final Counter accountsHandledCounter;

    public AccountCreatedHandler(AccountCreatedRepository accountCreatedRepository,
                                 MeterRegistry meterRegistry) {
        this.accountCreatedRepository = accountCreatedRepository;
        this.accountsHandledCounter = Counter.builder("accounts_handled_total")
                .register(meterRegistry);
    }

    public void handle(AccountCreated accountCreated) {
        accountCreatedRepository.save(accountCreated);
        accountsHandledCounter.increment();
    }

    public void handle(Collection<AccountCreated> accountsCreated) {
        accountCreatedRepository.save(accountsCreated);
        accountsHandledCounter.increment(accountsCreated.size());
    }

    public int accountsHandledCount() {
        return (int) accountsHandledCounter.count();
    }
}
