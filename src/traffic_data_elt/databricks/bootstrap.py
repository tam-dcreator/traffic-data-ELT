"""Reusable Databricks runtime bootstrap operations.

Packaged, importable building blocks for provisioning the Databricks runtime
infrastructure as V2 pipeline run needs, so an Airflow DAG can orchestrate them
without embedding build/upload/volume logic in DAG source and without shelling
out to the CLI scripts directly:

    * :func:`ensure_artifact_volume` — persistent UC volume for versioned wheels
    * :func:`ensure_versioned_wheel`  — build+upload the exact version+SHA wheel,
      reuse if it already exists (never rebuild/overwrite an existing artifact)
    * :func:`ensure_temp_volume`      — the temporary ``v2_temp`` working volume

Design
------
All external commands (``databricks``, ``pip``, ``git``) go through a small
:class:`CommandRunner` seam so the operations are unit-testable with an injected
fake runner — no live Databricks, network, or build required.  The default
runner shells out with ``DATABRICKS_HOST`` stripped from the child environment
(sourcing ``.env`` may set a placeholder host that would otherwise override the
chosen CLI profile).

Nothing here reads or logs secrets: only non-secret identifiers (catalog,
schema, volume, wheel name, git SHA, artifact path) are handled.

Wheel identity
--------------
The deployed artifact is versioned by project version **and** git SHA::

    traffic_data_elt-<version>-<git_sha>-py3-none-any.whl

so a given commit maps to exactly one immutable artifact.  ``pip wheel`` always
produces the un-suffixed ``traffic_data_elt-<version>-py3-none-any.whl``; the
SHA-stamped name is applied on upload.  If the SHA-stamped artifact already
exists in the artifact volume the build+upload is skipped (idempotent reuse).
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

# Package distribution name as it appears in the built wheel filename.
_DIST_NAME = "traffic_data_elt"

# Volume that must never be treated as a temporary/working volume.
_ARTIFACT_VOLUME_DEFAULT = "v2_artifacts"


# ---------------------------------------------------------------------------
# Command runner seam (injectable for tests)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CommandResult:
    """Result of an external command invocation."""

    returncode: int
    stdout: str = ""
    stderr: str = ""

    @property
    def ok(self) -> bool:
        return self.returncode == 0


# A runner takes an argv list and returns a CommandResult.
CommandRunner = Callable[[list[str]], CommandResult]


def default_command_runner(cmd: list[str], *, timeout: int = 900) -> CommandResult:
    """Run *cmd* as a subprocess with ``DATABRICKS_HOST`` stripped.

    Captures stdout/stderr as text.  Never raises on non-zero exit — the caller
    inspects :pyattr:`CommandResult.returncode`.
    """
    env = dict(os.environ)
    env.pop("DATABRICKS_HOST", None)
    try:
        p = subprocess.run(
            cmd, capture_output=True, text=True, env=env, timeout=timeout
        )
        return CommandResult(p.returncode, p.stdout, p.stderr)
    except (subprocess.SubprocessError, OSError) as exc:  # pragma: no cover - env dependent
        return CommandResult(1, "", str(exc))


def _databricks_argv(profile: str, *args: str) -> list[str]:
    """Build a ``databricks`` argv, honouring the implicit-DEFAULT convention.

    The implicit ``[DEFAULT]`` profile is selected by OMITTING ``--profile``;
    passing ``--profile DEFAULT`` fails to resolve on the Databricks CLI.
    """
    prefix = [] if profile.upper() in ("", "DEFAULT") else ["--profile", profile]
    return ["databricks", *prefix, *args]


# ---------------------------------------------------------------------------
# Pure helpers (no external commands)
# ---------------------------------------------------------------------------


def parse_volume_path(volume_path: str) -> tuple[str, str, str]:
    """Return ``(catalog, schema, volume)`` from a UC volume path.

    Accepts either a ``/Volumes/<cat>/<sch>/<vol>[/...]`` path or a bare
    ``<cat>.<sch>.<vol>`` fully-qualified name.

    Raises
    ------
    ValueError
        If the path does not contain catalog/schema/volume components.
    """
    p = volume_path.strip()
    if p.startswith("dbfs:"):
        p = p[len("dbfs:"):]
    parts = p.strip("/").split("/")
    if parts and parts[0] == "Volumes":
        parts = parts[1:]
    if len(parts) >= 3 and "." not in parts[0]:
        return parts[0], parts[1], parts[2]
    # Bare fully-qualified name "cat.sch.vol".
    dotted = p.strip("/").split(".")
    if len(dotted) == 3 and all(dotted):
        return dotted[0], dotted[1], dotted[2]
    raise ValueError(
        f"cannot parse catalog/schema/volume from {volume_path!r}; expected "
        f"/Volumes/<cat>/<sch>/<vol> or <cat>.<sch>.<vol>"
    )


def versioned_wheel_name(version: str, git_sha: str) -> str:
    """Return the SHA-stamped wheel filename for *version* + *git_sha*.

    Example: ``versioned_wheel_name("0.1.0", "abc1234")`` →
    ``"traffic_data_elt-0.1.0-abc1234-py3-none-any.whl"``.
    """
    if not version:
        raise ValueError("version is required")
    sha = git_sha or "nogit"
    return f"{_DIST_NAME}-{version}-{sha}-py3-none-any.whl"


# ---------------------------------------------------------------------------
# Bootstrap operations
# ---------------------------------------------------------------------------


def resolve_git_sha(
    repo_root: str | Path, *, runner: CommandRunner = default_command_runner
) -> str:
    """Return the short git SHA of *repo_root*, or ``"nogit"`` if unavailable."""
    res = runner(["git", "-C", str(repo_root), "rev-parse", "--short", "HEAD"])
    return res.stdout.strip() if res.ok and res.stdout.strip() else "nogit"


def _volume_exists(profile: str, catalog: str, schema: str, volume: str,
                   runner: CommandRunner) -> bool:
    res = runner(_databricks_argv(profile, "volumes", "read",
                                  f"{catalog}.{schema}.{volume}"))
    return res.ok


def ensure_artifact_volume(
    profile: str,
    artifact_path: str,
    *,
    runner: CommandRunner = default_command_runner,
) -> str:
    """Ensure the persistent UC artifact volume for *artifact_path* exists.

    Idempotent: reuses the volume if present, otherwise creates it as
    ``MANAGED``.  The artifact volume holds persistent versioned wheels and is
    NEVER dropped by pipeline cleanup.

    Returns
    -------
    str
        The fully-qualified volume name ``<catalog>.<schema>.<volume>``.

    Raises
    ------
    RuntimeError
        If the volume is absent and cannot be created.
    """
    catalog, schema, volume = parse_volume_path(artifact_path)
    fqn = f"{catalog}.{schema}.{volume}"
    if _volume_exists(profile, catalog, schema, volume, runner):
        log.info("artifact volume exists: %s (reused)", fqn)
        return fqn
    created = runner(_databricks_argv(profile, "volumes", "create",
                                      catalog, schema, volume, "MANAGED"))
    if not created.ok:
        raise RuntimeError(
            f"could not create artifact volume {fqn}: {created.stderr.strip()}"
        )
    log.info("artifact volume created: %s (MANAGED)", fqn)
    return fqn


def ensure_temp_volume(
    profile: str,
    temp_volume: str,
    *,
    runner: CommandRunner = default_command_runner,
) -> str:
    """Ensure the temporary working volume (``v2_temp``) exists.

    Idempotent: reuses if present, else creates ``MANAGED``.  A prior successful
    run may have dropped ``v2_temp``, so every new run must be able to recreate
    it automatically — this is that recreate step.

    *temp_volume* may be ``/Volumes/<cat>/<sch>/<vol>`` or ``<cat>.<sch>.<vol>``.

    Refuses to create the artifact volume by name (guards a misconfiguration
    that would confuse persistent and temporary storage).
    """
    catalog, schema, volume = parse_volume_path(temp_volume)
    if volume == _ARTIFACT_VOLUME_DEFAULT:
        raise ValueError(
            f"refusing to treat artifact volume {volume!r} as a temp volume"
        )
    fqn = f"{catalog}.{schema}.{volume}"
    if _volume_exists(profile, catalog, schema, volume, runner):
        log.info("temp volume exists: %s (reused)", fqn)
        return fqn
    created = runner(_databricks_argv(profile, "volumes", "create",
                                      catalog, schema, volume, "MANAGED"))
    if not created.ok:
        raise RuntimeError(
            f"could not create temp volume {fqn}: {created.stderr.strip()}"
        )
    log.info("temp volume created: %s (MANAGED)", fqn)
    return fqn


def wheel_exists(
    profile: str,
    artifact_dir: str,
    wheel_name: str,
    *,
    runner: CommandRunner = default_command_runner,
) -> bool:
    """True if *wheel_name* is already present in *artifact_dir* on Databricks."""
    dest_dir = artifact_dir.rstrip("/")
    res = runner(_databricks_argv(profile, "fs", "ls", f"dbfs:{dest_dir}"))
    return res.ok and wheel_name in res.stdout


@dataclass(frozen=True)
class WheelDeployResult:
    """Outcome of :func:`ensure_versioned_wheel`."""

    wheel_name: str
    wheel_path: str       # artifact-dir path (no dbfs: prefix), for WHEEL_PATH
    version: str
    git_sha: str
    reused: bool             # True when the exact wheel already existed


def build_wheel(
    repo_root: str | Path, *, runner: CommandRunner = default_command_runner
) -> Path:
    """Build the project wheel into ``dist/`` and return the newest wheel path.

    Raises ``RuntimeError`` on build failure or if no wheel is produced.
    """
    repo_root = Path(repo_root)
    dist = repo_root / "dist"
    res = runner(["python", "-m", "pip", "wheel", ".", "-w", str(dist), "--no-deps"])
    if not res.ok:
        raise RuntimeError(
            f"wheel build failed: {res.stderr.strip() or res.stdout.strip()}"
        )
    wheels = sorted(dist.glob(f"{_DIST_NAME}-*.whl"), key=lambda w: w.stat().st_mtime)
    if not wheels:
        raise RuntimeError("no wheel produced in dist/")
    return wheels[-1]


def validate_wheel_contents(wheel_file: str | Path) -> list[str]:
    """Return the required package modules missing from *wheel_file*.

    Confirms the wheel carries ``traffic_data_elt`` and the
    ``traffic_data_elt/databricks`` runtime package, and that it does NOT bundle
    ``.env`` or the dbt ``target/`` directory.  Returns a list of problems; an
    empty list means the wheel is valid.
    """
    import zipfile  # noqa: PLC0415 - stdlib, only needed here

    problems: list[str] = []
    with zipfile.ZipFile(wheel_file, "r") as zf:
        names = zf.namelist()
    has_pkg = any(n.startswith("traffic_data_elt/") for n in names)
    has_db = any(n.startswith("traffic_data_elt/databricks/") for n in names)
    if not has_pkg:
        problems.append("missing traffic_data_elt package")
    if not has_db:
        problems.append("missing traffic_data_elt.databricks runtime package")
    # Must not leak env or build artifacts.
    for bad in (".env", "dbt/traffic_dwh/target/"):
        if any(bad in n for n in names):
            problems.append(f"wheel unexpectedly contains {bad!r}")
    return problems


def ensure_versioned_wheel(
    profile: str,
    artifact_dir: str,
    *,
    repo_root: str | Path,
    version: str,
    git_sha: str | None = None,
    runner: CommandRunner = default_command_runner,
) -> WheelDeployResult:
    """Ensure the exact version+SHA wheel exists in *artifact_dir*.

    Reuse-if-exists: if the SHA-stamped wheel is already present, do nothing
    (no rebuild, no overwrite).  Otherwise build the wheel, validate its
    contents, upload it under the SHA-stamped name, and verify the upload.

    Returns a :class:`WheelDeployResult` (its ``wheel_path`` is the value to
    pass to jobs as ``WHEEL_PATH``).
    """
    sha = git_sha if git_sha is not None else resolve_git_sha(repo_root, runner=runner)
    wheel_name = versioned_wheel_name(version, sha)
    dest_dir = artifact_dir.rstrip("/")
    wheel_path = f"{dest_dir}/{wheel_name}"

    if wheel_exists(profile, dest_dir, wheel_name, runner=runner):
        log.info("versioned wheel exists: %s (reused, no rebuild)", wheel_name)
        return WheelDeployResult(wheel_name, wheel_path, version, sha, reused=True)

    # Build (produces the un-suffixed name) then upload under the SHA name.
    built = build_wheel(repo_root, runner=runner)
    problems = validate_wheel_contents(built)
    if problems:
        raise RuntimeError(f"wheel content validation failed: {problems}")

    # Ensure the artifact sub-directory exists (idempotent).
    runner(_databricks_argv(profile, "fs", "mkdirs", f"dbfs:{dest_dir}"))
    dest = f"dbfs:{wheel_path}"
    up = runner(_databricks_argv(profile, "fs", "cp", str(built), dest, "--overwrite"))
    if not up.ok:
        raise RuntimeError(f"wheel upload failed: {up.stderr.strip()}")

    if not wheel_exists(profile, dest_dir, wheel_name, runner=runner):
        raise RuntimeError("wheel not present after upload (verification failed)")

    log.info("versioned wheel deployed: %s", wheel_name)
    return WheelDeployResult(wheel_name, wheel_path, version, sha, reused=False)


# ---------------------------------------------------------------------------
# Databricks notebook deployment
# ---------------------------------------------------------------------------

# The notebooks the production pipeline submits, mapped to their workspace name.
# Each source file under the notebooks dir imports to <workspace_dir>/<stem>.
DEFAULT_NOTEBOOKS = ("silver_pipeline", "gold_pipeline", "serving_pipeline")

# Databricks prepends this magic header when a Python file is stored as a
# notebook. It is normalised away when comparing deployed vs local content.
_DATABRICKS_NB_HEADER = "# Databricks notebook source"


@dataclass(frozen=True)
class NotebookDeployResult:
    """Per-notebook deployment outcome."""

    name: str
    workspace_path: str
    action: str  # "imported" | "updated" | "unchanged"


def _normalise_notebook_source(text: str) -> str:
    """Return notebook body without the Databricks magic header / trailing WS.

    Databricks' SOURCE export prepends ``# Databricks notebook source`` and may
    normalise trailing whitespace; stripping the header + trailing blank lines
    lets a local file and its deployed export compare equal when unchanged.
    """
    lines = text.replace("\r\n", "\n").split("\n")
    if lines and lines[0].strip() == _DATABRICKS_NB_HEADER:
        lines = lines[1:]
    # Drop leading/trailing blank lines for a stable comparison.
    return "\n".join(lines).strip()


def _workspace_object_exists(profile: str, workspace_path: str,
                             runner: CommandRunner) -> bool:
    res = runner(_databricks_argv(profile, "workspace", "get-status", workspace_path))
    return res.ok


def _deployed_notebook_source(profile: str, workspace_path: str,
                              runner: CommandRunner) -> str | None:
    """Return the deployed notebook's SOURCE export, or None if absent/unreadable."""
    res = runner(_databricks_argv(
        profile, "workspace", "export", workspace_path, "--format", "SOURCE"
    ))
    if not res.ok:
        return None
    return res.stdout


def ensure_databricks_notebooks(
    profile: str,
    notebooks_dir: str | Path,
    *,
    workspace_dir: str = "/traffic_data",
    notebooks: tuple[str, ...] = DEFAULT_NOTEBOOKS,
    runner: CommandRunner = default_command_runner,
) -> list[NotebookDeployResult]:
    """Idempotently deploy the pipeline notebooks into the Databricks workspace.

    For each notebook the operation:

    1. ensures the *workspace_dir* (e.g. ``/traffic_data``) exists,
    2. imports the notebook if it is missing,
    3. overwrites it if the repository version differs from the deployed one
       (compared on normalised SOURCE content, ignoring the Databricks header),
    4. leaves it untouched if identical, and
    5. verifies each notebook exists after deployment.

    Does not assume the workspace directory was created manually.  Uses the
    selected Databricks CLI *profile*; never embeds credentials.

    Returns one :class:`NotebookDeployResult` per notebook.

    Raises
    ------
    FileNotFoundError
        If a source notebook file is missing under *notebooks_dir*.
    RuntimeError
        If the directory cannot be created, an import fails, or a notebook is
        absent after deployment (verification failure).
    """
    notebooks_dir = Path(notebooks_dir)
    wsdir = workspace_dir.rstrip("/") or "/traffic_data"

    # 1. Ensure the workspace directory exists (idempotent).
    mk = runner(_databricks_argv(profile, "workspace", "mkdirs", wsdir))
    if not mk.ok:
        raise RuntimeError(f"could not create workspace dir {wsdir}: {mk.stderr.strip()}")

    results: list[NotebookDeployResult] = []
    for name in notebooks:
        local_file = notebooks_dir / f"{name}.py"
        if not local_file.is_file():
            raise FileNotFoundError(f"notebook source not found: {local_file}")
        ws_path = f"{wsdir}/{name}"
        local_src = _normalise_notebook_source(local_file.read_text())

        exists = _workspace_object_exists(profile, ws_path, runner)
        action = "imported"
        if exists:
            deployed = _deployed_notebook_source(profile, ws_path, runner)
            if deployed is not None and _normalise_notebook_source(deployed) == local_src:
                log.info("notebook unchanged: %s", ws_path)
                # Still verify existence below.
                results.append(NotebookDeployResult(name, ws_path, "unchanged"))
                continue
            action = "updated"

        imp = runner(_databricks_argv(
            profile, "workspace", "import", ws_path,
            "--file", str(local_file), "--language", "PYTHON",
            "--format", "SOURCE", "--overwrite",
        ))
        if not imp.ok:
            raise RuntimeError(f"notebook import failed for {ws_path}: {imp.stderr.strip()}")

        # 5. Verify the notebook exists after deployment.
        if not _workspace_object_exists(profile, ws_path, runner):
            raise RuntimeError(f"notebook not present after import: {ws_path}")

        log.info("notebook %s: %s", action, ws_path)
        results.append(NotebookDeployResult(name, ws_path, action))

    return results
