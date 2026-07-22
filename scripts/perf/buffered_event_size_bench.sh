#!/usr/bin/env bash
set -euo pipefail

go test -tags test_dep ./service/history/workflow/workflow_test \
  -run '^$' \
  -bench '^BenchmarkBufferSizeAcceptableActiveSignalCadence$' \
  -count=1 \
  -benchtime=250ms \
  -benchmem \
  -cpu=1 \
  | perfloop-go-bench-json 'BenchmarkBufferSizeAcceptableActiveSignalCadence' 'ns/op' 'B/op' 'allocs/op'
