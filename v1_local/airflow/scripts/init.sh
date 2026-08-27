#!/usr/bin/env bash
# airflow-init bootstrap script.
# Runs DB migrations and seeds the SimpleAuthManager password file.
# Executed by the airflow-init service defined in compose.yaml.

set -euo pipefail

echo "[init] Running Airflow DB migrations…"
airflow db migrate

echo "[init] Seeding SimpleAuthManager password file…"
python - <<'EOF'
import json, os, pathlib


pw_file = pathlib.Path(
    os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]
)
pw_file.parent.mkdir(parents=True, exist_ok=True)
assert pw_file.is_absolute(), f"Password file path must be absolute: {pw_file}"

# Username is the first element of the "username:role" pair.
username = os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"].split(":")[0].strip()
password = os.environ["_AIRFLOW_ADMIN_PASSWORD"]

existing = {}
if pw_file.exists():
    try:
        existing = json.loads(pw_file.read_text())
    except json.JSONDecodeError:
        pass

existing[username] = password
pw_file.write_text(json.dumps(existing))
print(f"[init] Password written for user: {username}")
EOF

chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/auth

echo "[init] Airflow init complete."
