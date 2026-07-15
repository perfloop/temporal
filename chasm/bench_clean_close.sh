#!/usr/bin/env bash
set -euo pipefail

GOMODCACHE=/workspace/deps/gomodcache \
GOCACHE="$PWD/.perfloop-go-build" \
go test ./chasm -run '^$' -bench '^BenchmarkCloseTransactionCleanPersistedTree$' -count=1 -benchmem \
  | perfloop-go-bench-json 'BenchmarkCloseTransactionCleanPersistedTree' 'ns/op' 'B/op' 'allocs/op'
