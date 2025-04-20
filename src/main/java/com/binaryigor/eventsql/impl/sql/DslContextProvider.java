package com.binaryigor.eventsql.impl.sql;

import org.jooq.DSLContext;

public interface DslContextProvider {
    DSLContext get();
}
