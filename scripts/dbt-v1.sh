#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

set -a
source "$ROOT/dbt/traffic_dwh/.env"
set +a

export DBT_PROFILES_DIR="$ROOT/dbt/traffic_dwh"

cd "$ROOT/dbt/traffic_dwh"

exec dbt "$@"
