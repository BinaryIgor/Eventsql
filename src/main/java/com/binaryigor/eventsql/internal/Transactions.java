package com.binaryigor.eventsql.internal;

public interface Transactions {
    void execute(Runnable transaction);
}
