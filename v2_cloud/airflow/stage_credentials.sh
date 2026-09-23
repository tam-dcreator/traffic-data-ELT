#!/usr/bin/env bash
# Stage host credentials for the V2 Airflow containers (read-only mounts).
#
# WHY THIS EXISTS
# ---------------
# The Airflow containers run as uid 50000, group 0 (root). The developer's real
# credential files live in $HOME as mode 0600 owned by the dev-container user
# (uid 1000): ~/.databrickscfg and ~/.aws/{config,credentials}. A read-only bind
# mount preserves those 0600 permissions, so uid 50000 could NOT read them.
#
# This script copies (does not move) those credentials into a git-ignored
# runtime staging directory with group-0 read permission (mode 0640, group
# root), so the airflow user — a member of group 0 — can read them, while
# "other" still cannot. The originals are untouched.
#
# The staging directory is mounted READ-ONLY into the containers and is
# git-ignored (see v2_cloud/airflow/.gitignore). Nothing here is committed.
#
# Re-run this whenever you refresh `databricks auth login` or rotate AWS creds.
#
# Usage:
#   bash v2_cloud/airflow/stage_credentials.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STAGE_DIR="${SCRIPT_DIR}/.runtime-credentials"
SRC_DATABRICKS="${HOME}/.databrickscfg"
SRC_DATABRICKS_DIR="${HOME}/.databricks"   # holds the OAuth token-cache.json
SRC_AWS_DIR="${HOME}/.aws"

# The gid the Airflow container's user belongs to (compose: user "50000:0").
AIRFLOW_GID=0

echo "[stage] staging runtime credentials into ${STAGE_DIR}"
rm -rf "${STAGE_DIR}"
mkdir -p "${STAGE_DIR}/.aws" "${STAGE_DIR}/.databricks"

missing=0

# The Airflow containers run as uid 50000, group 0. The dev-container user
# (uid 1000) cannot chgrp to group 0 without privilege, so we use
# passwordless sudo (available in the dev container) purely to set group
# ownership to 0 on the STAGED COPIES. The developer's real dotfiles are never
# touched. If sudo is unavailable, we fall back to world-readable staged copies
# (still confined to the git-ignored staging dir inside the isolated container).
_chgrp0() {  # _chgrp0 <path>
  if sudo -n true 2>/dev/null; then
    sudo chgrp -R "${AIRFLOW_GID}" "$1"
    return 0
  fi
  return 1
}

# ── Databricks CLI profile (~/.databrickscfg) ────────────────────────────────
if [[ -f "${SRC_DATABRICKS}" ]]; then
  install -m 0640 "${SRC_DATABRICKS}" "${STAGE_DIR}/.databrickscfg"
  echo "[stage]   databrickscfg staged"
else
  echo "[stage]   WARN: ${SRC_DATABRICKS} not found — run 'databricks auth login' first" >&2
  missing=1
fi

# ── Databricks OAuth token cache (~/.databricks/token-cache.json) ────────────
# The databricks-cli auth_type stores its OAuth access/refresh tokens here, NOT
# in .databrickscfg. Without this the container cannot authenticate.
if [[ -f "${SRC_DATABRICKS_DIR}/token-cache.json" ]]; then
  # GROUP-WRITABLE: the Databricks CLI refreshes OAuth tokens and must persist
  # them back to this cache, so this dir/file is mounted READ-WRITE and must be
  # writable by the airflow user (group 0). Writes land on the staged copy only,
  # never the host original.
  install -m 0660 "${SRC_DATABRICKS_DIR}/token-cache.json" \
    "${STAGE_DIR}/.databricks/token-cache.json"
  chmod 0770 "${STAGE_DIR}/.databricks"
  echo "[stage]   databricks token-cache staged (group-writable for token refresh)"
else
  echo "[stage]   WARN: ${SRC_DATABRICKS_DIR}/token-cache.json not found — run 'databricks auth login' first" >&2
  missing=1
fi

# ── AWS config + credentials (~/.aws) ────────────────────────────────────────
if [[ -d "${SRC_AWS_DIR}" ]]; then
  for f in config credentials; do
    if [[ -f "${SRC_AWS_DIR}/${f}" ]]; then
      install -m 0640 "${SRC_AWS_DIR}/${f}" "${STAGE_DIR}/.aws/${f}"
      echo "[stage]   aws/${f} staged"
    fi
  done
  # Directory itself must be group-traversable by the airflow user.
  chmod 0750 "${STAGE_DIR}/.aws"
else
  echo "[stage]   WARN: ${SRC_AWS_DIR} not found — configure AWS credentials first" >&2
  missing=1
fi

chmod 0750 "${STAGE_DIR}"

# Set group 0 on the staged copies so the airflow user (group 0) can read them
# with mode 0640 (most-restrictive that works). Fall back to world-readable.
if _chgrp0 "${STAGE_DIR}"; then
  echo "[stage]   staged copies owned by group ${AIRFLOW_GID} (mode 0640; other cannot read)"
else
  echo "[stage]   WARN: sudo unavailable — making staged copies world-readable (0644)" >&2
  chmod -R o+r "${STAGE_DIR}"
  find "${STAGE_DIR}" -type d -exec chmod o+x {} +
fi

if [[ "${missing}" -eq 1 ]]; then
  echo "[stage] completed WITH WARNINGS (some credentials missing)." >&2
else
  echo "[stage] done. Credentials staged read-only for the Airflow containers."
fi
