#!/bin/bash
set -euo pipefail
events=${BENCHMARK_EVENTS:-10000}
per_second_rate=${BENCHMARK_PER_SECOND_RATE:-500}
curl -X POST "http://localhost:8080/benchmarks/run?events=$events&perSecondRate=$per_second_rate"
