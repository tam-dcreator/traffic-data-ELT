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

home    = pathlib.Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
pw_file = home / "simple_auth_manager_passwords.json.generated"

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

echo "[init] Airflow init complete."
