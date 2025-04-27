package com.binaryigor.eventsql.benchmarks;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BenchmarksController {

    private final Benchmarks benchmarks;

    public BenchmarksController(Benchmarks benchmarks) {
        this.benchmarks = benchmarks;
    }

    @PostMapping("/benchmarks/run")
    @ResponseStatus(HttpStatus.ACCEPTED)
    void run(@RequestParam("events") int events,
             @RequestParam("perSecondRate") int perSecondRate,
             @RequestParam("batchConsumer") boolean batchConsumer) {
        Thread.startVirtualThread(() -> benchmarks.run(events, perSecondRate, batchConsumer));
    }
}
