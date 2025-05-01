package com.binaryigor.eventsql.internal.sql;

import org.jooq.DSLContext;

public interface DslContextProvider {
    DSLContext get();
}
