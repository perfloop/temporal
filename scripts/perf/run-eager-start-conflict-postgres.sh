#!/usr/bin/env bash
# Starts an isolated PostgreSQL server for the eager-start conflict proof.
# The server binary cache contains no repository source; the cluster and all
# test output remain under the current worktree and are removed on exit.
set -euo pipefail

repo_root=$(pwd)
deps_root=${PERFLOOP_DEPS_DIR:-/workspace/deps}
postgres_version=17.10-0+deb13u1
postgres_root="$deps_root/temporal-eager-start-postgres-$postgres_version"
postgres_bin="$postgres_root/server/usr/lib/postgresql/17/bin"
postgres_lib="$postgres_root/libpq/usr/lib/x86_64-linux-gnu"

prepare_postgres() {
	if [[ -x "$postgres_bin/initdb" && -x "$postgres_bin/pg_ctl" && -d "$postgres_lib" ]]; then
		return
	fi

	mkdir -p "$deps_root"
	local staging
	staging=$(mktemp -d "$deps_root/.temporal-eager-start-postgres.XXXXXX")
	local base_url=https://deb.debian.org/debian/pool/main/p/postgresql-17
	local server_deb="$staging/postgresql-17.deb"
	local libpq_deb="$staging/libpq5.deb"
	curl --fail --location --silent --show-error \
		"${base_url}/postgresql-17_${postgres_version}_amd64.deb" \
		--output "$server_deb"
	curl --fail --location --silent --show-error \
		"${base_url}/libpq5_${postgres_version}_amd64.deb" \
		--output "$libpq_deb"
	echo '3b7d9dbfd2f618d767fc091ffb7432faa6284a20f7095b430dd57644602f40dd  postgresql-17.deb' | (cd "$staging" && sha256sum --check --status -)
	echo 'bcaba7700a2afbdc4b7bf0b0bc9532f1cd49a8fd6fa47ccab125befd4ba7716a  libpq5.deb' | (cd "$staging" && sha256sum --check --status -)
	dpkg-deb --extract "$server_deb" "$staging/server"
	dpkg-deb --extract "$libpq_deb" "$staging/libpq"
	if [[ ! -d "$postgres_root" ]]; then
		mv "$staging" "$postgres_root"
	else
		rm -rf "$staging"
	fi
}

if [[ ${1:-} == --prepare ]]; then
	prepare_postgres
	exit 0
fi

prepare_postgres
export LD_LIBRARY_PATH="$postgres_lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

work_dir=$(mktemp -d "$repo_root/.perfloop-postgres.XXXXXX")
data_dir="$work_dir/data"
log_file="$work_dir/postgres.log"
port=$(python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)

cleanup() {
	local status=$?
	set +e
	"$postgres_bin/pg_ctl" -D "$data_dir" -m immediate stop >>"$log_file" 2>&1
	if (( status != 0 )); then
		cat "$log_file" >&2
	fi
	rm -rf "$work_dir"
	return "$status"
}
trap cleanup EXIT

"$postgres_bin/initdb" -D "$data_dir" --no-locale --encoding=UTF8 --auth=trust --username=temporal >"$log_file" 2>&1
"$postgres_bin/pg_ctl" -D "$data_dir" -l "$log_file" -o "-h 127.0.0.1 -k $work_dir -p $port -F" -w start >>"$log_file" 2>&1

export TEMPORAL_EAGER_START_CONFLICT_POSTGRES_ADDR="127.0.0.1:$port"
export TEMPORAL_EAGER_START_CONFLICT_POSTGRES_SCHEMA="$repo_root/schema/postgresql/v12/temporal/schema.sql"
# Test resources construct a logger for each benchmark calibration run. Keep
# those diagnostics off the benchmark stream so the native Go row remains
# parseable by perfloop-go-bench-json.
export TEMPORAL_TEST_LOG_LEVEL=error

if [[ ${1:-} == --test ]]; then
	go test ./service/history -run '^TestEagerStartConflictReturnsInitialEvents$' -count=1
	exit 0
fi

if [[ -n ${PERFLOOP_BENCH_BIN:-} ]]; then
	"$PERFLOOP_BENCH_BIN" -test.bench='^BenchmarkEagerStartConflict$' -test.run='^$' -test.benchtime=1s -test.benchmem
else
	go test -bench='^BenchmarkEagerStartConflict$' -run='^$' -count=1 -benchtime=1s -benchmem ./service/history
fi
