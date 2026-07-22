#!/usr/bin/env bash
set -euo pipefail

go test -tags test_dep ./service/history/workflow/workflow_test \
  -run '^$' \
  -bench '^BenchmarkBufferedEventBatchReload$' \
  -count=1 \
  -benchtime=1s \
  -benchmem \
  -cpu=1 \
  | perfloop-go-bench-json 'BenchmarkBufferedEventBatchReload' 'ns/op' 'B/op' 'allocs/op'
