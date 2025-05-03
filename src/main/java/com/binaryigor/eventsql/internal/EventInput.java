package com.binaryigor.eventsql.internal;

import com.binaryigor.eventsql.EventPublication;

public record EventInput(EventPublication publication, short partition) {
}
