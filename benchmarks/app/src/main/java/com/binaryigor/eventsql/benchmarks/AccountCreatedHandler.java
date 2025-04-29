package com.binaryigor.eventsql.benchmarks;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Random;

@Component
public class AccountCreatedHandler {

    private static final Random RANDOM = new Random();
    private final Counter accountsHandledCounter;

    public AccountCreatedHandler(MeterRegistry meterRegistry) {
        this.accountsHandledCounter = Counter.builder("accounts_handled_total")
                .register(meterRegistry);
    }

    public void handle(AccountCreated accountCreated) {
        handleDelay();
        accountsHandledCounter.increment();
    }

    public void handle(Collection<AccountCreated> accountsCreated) {
        handleDelay();
        accountsHandledCounter.increment(accountsCreated.size());
    }

    private void handleDelay() {
        try {
            Thread.sleep(1 + RANDOM.nextInt(100));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
