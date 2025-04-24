package com.binaryigor.eventsql.benchmarks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

@Component
public class Benchmarks {

    private static final String BENCHMARK_DELIMITER = "\n" + "-".repeat(64) + "\n";
    private static final Logger logger = LoggerFactory.getLogger(Benchmarks.class);
    private static final Random RANDOM = new Random();
    private final EventsPublisher eventsPublisher;
    private final AccountCreatedHandler accountCreatedHandler;

    public Benchmarks(EventsPublisher eventsPublisher,
                      AccountCreatedHandler accountCreatedHandler) {
        this.eventsPublisher = eventsPublisher;
        this.accountCreatedHandler = accountCreatedHandler;
    }

    public void run(int events, int perSecondRate) {
        var start = Instant.now();
        var consumedBefore = accountCreatedHandler.accountsHandledCount();

        logger.info(BENCHMARK_DELIMITER);
        logger.info("Starting next benchmark with {} events and {} perSecondRate", events, perSecondRate);
        logger.info("");

        var publishedEvents = 0;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            while (publishedEvents < events) {
                var toPublish = Math.min(events - publishedEvents, perSecondRate);
                logger.info("Publishing next {} random events per second...", toPublish);
                var latch = publishRandomEvents(executor, perSecondRate);

                if (toPublish > perSecondRate) {
                    Thread.sleep(1000);
                }
                latch.await();

                publishedEvents += perSecondRate;
            }
        } catch (Exception e) {
            logger.error("Problem while publishing events: ", e);
        }

        var publishingEnd = Instant.now();
        var publishingDuration = Duration.between(start, publishingEnd);
        logger.info("Publishing {} events with {} per second rate took: {}, which means {} per second rate",
                events, perSecondRate, publishingDuration, perSecondRate(publishingDuration, events));
        logger.info("Waiting for consumption....");

        while (true) {
            try {
                var consumed = accountCreatedHandler.accountsHandledCount() - consumedBefore;
                if (consumed >= events) {
                    break;
                }
                logger.info("{}/{} events were consumed, waiting...", consumed, events);
                Thread.sleep(1000);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        var consumptionEnd = Instant.now();
        var consumptionDuration = Duration.between(start, consumptionEnd);
        logger.info("Consuming {} events with {} per second rate took: {}, which means {} per second rate",
                events, perSecondRate, consumptionDuration, perSecondRate(consumptionDuration, events));
        logger.info(BENCHMARK_DELIMITER);
    }

    private CountDownLatch publishRandomEvents(Executor executor, int events) {
        var publishedLatch = new CountDownLatch(events);
        IntStream.range(0, events)
                .forEach($ -> {
                    var event = randomAccountCreatedEvent();
                    executor.execute(() -> {
                        try {
                            Thread.sleep(RANDOM.nextInt(1000));
                            eventsPublisher.publishAccountCreated(event);
                            publishedLatch.countDown();
                        } catch (Exception e) {
                            logger.error("Problem while publishing event: ", e);
                        }
                    });
                });
        return publishedLatch;
    }

    private AccountCreated randomAccountCreatedEvent() {
        var id = UUID.randomUUID();
        return new AccountCreated(id, id + "@email.com", id + "-name", Instant.now());
    }

    private double perSecondRate(Duration duration, int amount) {
        return BigDecimal.valueOf(amount * 1000.0 / duration.toMillis())
                .setScale(1, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
