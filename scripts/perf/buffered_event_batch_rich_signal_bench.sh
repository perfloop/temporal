#!/usr/bin/env bash
set -euo pipefail

go test -tags test_dep ./service/history/workflow/workflow_test \
  -run '^$' \
  -bench '^BenchmarkBufferedEventBatchRichSignalCadence$' \
  -count=1 \
  -benchtime=1s \
  -benchmem \
  -cpu=1 \
  | perfloop-go-bench-json 'BenchmarkBufferedEventBatchRichSignalCadence' 'ns/op' 'B/op' 'allocs/op'
