#!/bin/bash
set -euo pipefail
events=${BENCHMARK_EVENTS:-50000}
per_second_rate=${BENCHMARK_PER_SECOND_RATE:-500}
batch_consumer=${BATCH_CONSUMER:-true}
curl -X POST "http://localhost:8080/benchmarks/run?events=$events&perSecondRate=$per_second_rate&batchConsumer=$batch_consumer"
