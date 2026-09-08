"""Unit tests for traffic_data_elt.databricks.bootstrap.

Uses an injected fake CommandRunner so no live Databricks / pip / git is needed.
Covers: volume-path parsing, versioned wheel naming, git SHA resolution,
artifact-volume ensure (create vs reuse), temp-volume ensure (create vs reuse +
artifact-volume guard), versioned-wheel ensure (reuse-if-exists vs build+upload),
and wheel content validation.
"""

from __future__ import annotations

import zipfile

import pytest

from traffic_data_elt.databricks import bootstrap as b
from traffic_data_elt.databricks.bootstrap import CommandResult


class FakeRunner:
    """Records argv calls and returns queued/scripted CommandResults."""

    def __init__(self, responses: dict[str, CommandResult] | None = None,
                 default: CommandResult | None = None):
        self.calls: list[list[str]] = []
        self.responses = responses or {}
        self.default = default or CommandResult(0, "", "")

    def __call__(self, cmd: list[str]) -> CommandResult:
        self.calls.append(cmd)
        joined = " ".join(cmd)
        for key, res in self.responses.items():
            if key in joined:
                return res
        return self.default

    def called_with(self, fragment: str) -> bool:
        return any(fragment in " ".join(c) for c in self.calls)


# ── parse_volume_path ─────────────────────────────────────────────────────────

class TestParseVolumePath:
    def test_volumes_path(self):
        assert b.parse_volume_path("/Volumes/workspace/default/v2_artifacts/wheels") == (
            "workspace", "default", "v2_artifacts")

    def test_dbfs_prefixed(self):
        assert b.parse_volume_path("dbfs:/Volumes/c/s/v") == ("c", "s", "v")

    def test_dotted_fqn(self):
        assert b.parse_volume_path("workspace.default.v2_temp") == (
            "workspace", "default", "v2_temp")

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            b.parse_volume_path("not-a-volume")


# ── versioned_wheel_name ────────────────────────────────────────────────────────

class TestVersionedWheelName:
    def test_name(self):
        assert b.versioned_wheel_name("0.1.0", "abc1234") == (
            "traffic_data_elt-0.1.0-abc1234-py3-none-any.whl")

    def test_nogit_when_sha_empty(self):
        assert "nogit" in b.versioned_wheel_name("0.1.0", "")

    def test_requires_version(self):
        with pytest.raises(ValueError):
            b.versioned_wheel_name("", "abc")


# ── resolve_git_sha ─────────────────────────────────────────────────────────────

class TestResolveGitSha:
    def test_ok(self):
        r = FakeRunner(default=CommandResult(0, "deadbee\n", ""))
        assert b.resolve_git_sha("/repo", runner=r) == "deadbee"

    def test_failure_returns_nogit(self):
        r = FakeRunner(default=CommandResult(1, "", "fatal"))
        assert b.resolve_git_sha("/repo", runner=r) == "nogit"


# ── ensure_artifact_volume ──────────────────────────────────────────────────────

class TestEnsureArtifactVolume:
    def test_reuse_when_exists(self):
        r = FakeRunner(responses={"volumes read": CommandResult(0, "{}", "")})
        fqn = b.ensure_artifact_volume("DEFAULT",
                                       "/Volumes/workspace/default/v2_artifacts/wheels",
                                       runner=r)
        assert fqn == "workspace.default.v2_artifacts"
        assert not r.called_with("volumes create")

    def test_create_when_absent(self):
        r = FakeRunner(responses={
            "volumes read": CommandResult(1, "", "not found"),
            "volumes create": CommandResult(0, "", ""),
        })
        fqn = b.ensure_artifact_volume("DEFAULT",
                                       "/Volumes/workspace/default/v2_artifacts/wheels",
                                       runner=r)
        assert fqn == "workspace.default.v2_artifacts"
        assert r.called_with("volumes create workspace default v2_artifacts MANAGED")

    def test_create_failure_raises(self):
        r = FakeRunner(responses={
            "volumes read": CommandResult(1, "", "not found"),
            "volumes create": CommandResult(1, "", "denied"),
        })
        with pytest.raises(RuntimeError):
            b.ensure_artifact_volume("DEFAULT", "/Volumes/c/s/v2_artifacts", runner=r)


# ── ensure_temp_volume ──────────────────────────────────────────────────────────

class TestEnsureTempVolume:
    def test_reuse_when_exists(self):
        r = FakeRunner(responses={"volumes read": CommandResult(0, "{}", "")})
        fqn = b.ensure_temp_volume("DEFAULT", "workspace.default.v2_temp", runner=r)
        assert fqn == "workspace.default.v2_temp"
        assert not r.called_with("volumes create")

    def test_create_when_absent(self):
        r = FakeRunner(responses={
            "volumes read": CommandResult(1, "", "not found"),
            "volumes create": CommandResult(0, "", ""),
        })
        fqn = b.ensure_temp_volume("DEFAULT", "/Volumes/workspace/default/v2_temp",
                                   runner=r)
        assert fqn == "workspace.default.v2_temp"
        assert r.called_with("volumes create workspace default v2_temp MANAGED")

    def test_refuses_artifact_volume(self):
        r = FakeRunner()
        with pytest.raises(ValueError, match="artifact volume"):
            b.ensure_temp_volume("DEFAULT", "workspace.default.v2_artifacts", runner=r)


# ── ensure_versioned_wheel ──────────────────────────────────────────────────────

class TestEnsureVersionedWheel:
    def test_reuse_when_wheel_exists(self):
        wheel = b.versioned_wheel_name("0.1.0", "abc1234")
        # fs ls returns the wheel name → reuse, no build.
        r = FakeRunner(responses={"fs ls": CommandResult(0, wheel, "")})
        result = b.ensure_versioned_wheel(
            "DEFAULT", "/Volumes/workspace/default/v2_artifacts/wheels",
            repo_root="/repo", version="0.1.0", git_sha="abc1234", runner=r,
        )
        assert result.reused is True
        assert result.wheel_name == wheel
        assert not r.called_with("pip wheel")
        assert not r.called_with("fs cp")

    def test_build_and_upload_when_absent(self, tmp_path, monkeypatch):
        wheel = b.versioned_wheel_name("0.1.0", "abc1234")
        # First fs ls (existence check) empty; build succeeds; verify fs ls returns wheel.
        calls = {"n": 0}

        # Create a fake built wheel that passes content validation.
        dist = tmp_path / "dist"
        dist.mkdir()
        built = dist / "traffic_data_elt-0.1.0-py3-none-any.whl"
        with zipfile.ZipFile(built, "w") as zf:
            zf.writestr("traffic_data_elt/__init__.py", "")
            zf.writestr("traffic_data_elt/databricks/__init__.py", "")

        def runner(cmd: list[str]) -> CommandResult:
            joined = " ".join(cmd)
            if "fs ls" in joined:
                calls["n"] += 1
                # First existence check: absent. Post-upload verify: present.
                return CommandResult(0, wheel if calls["n"] > 1 else "", "")
            if "pip wheel" in joined:
                return CommandResult(0, "built", "")
            return CommandResult(0, "", "")

        result = b.ensure_versioned_wheel(
            "DEFAULT", "/Volumes/workspace/default/v2_artifacts/wheels",
            repo_root=tmp_path, version="0.1.0", git_sha="abc1234", runner=runner,
        )
        assert result.reused is False
        assert result.wheel_name == wheel


# ── validate_wheel_contents ─────────────────────────────────────────────────────

class TestValidateWheelContents:
    def _make_wheel(self, path, names):
        with zipfile.ZipFile(path, "w") as zf:
            for n in names:
                zf.writestr(n, "")
        return path

    def test_valid(self, tmp_path):
        w = self._make_wheel(tmp_path / "w.whl", [
            "traffic_data_elt/__init__.py",
            "traffic_data_elt/databricks/bootstrap.py",
        ])
        assert b.validate_wheel_contents(w) == []

    def test_missing_databricks_pkg(self, tmp_path):
        w = self._make_wheel(tmp_path / "w.whl", ["traffic_data_elt/__init__.py"])
        problems = b.validate_wheel_contents(w)
        assert any("databricks" in p for p in problems)

    def test_rejects_env_leak(self, tmp_path):
        w = self._make_wheel(tmp_path / "w.whl", [
            "traffic_data_elt/__init__.py",
            "traffic_data_elt/databricks/x.py",
            ".env",
        ])
        problems = b.validate_wheel_contents(w)
        assert any(".env" in p for p in problems)


# ── ensure_databricks_notebooks ─────────────────────────────────────────────────

class TestEnsureDatabricksNotebooks:
    def _make_notebooks(self, tmp_path, names=b.DEFAULT_NOTEBOOKS):
        d = tmp_path / "notebooks"
        d.mkdir()
        for n in names:
            (d / f"{n}.py").write_text(f"# {n}\nimport os\n")
        return d

    def test_creates_dir_and_imports_when_missing(self, tmp_path):
        d = self._make_notebooks(tmp_path)
        # get-status fails (absent) → import each; post-import get-status ok.
        calls = {"status": 0}

        def runner(cmd):
            joined = " ".join(cmd)
            if "workspace mkdirs" in joined:
                return CommandResult(0, "", "")
            if "workspace get-status" in joined:
                calls["status"] += 1
                # Absent on the pre-import check, present on the verify check.
                return CommandResult(0 if calls["status"] % 2 == 0 else 1, "", "")
            if "workspace import" in joined:
                return CommandResult(0, "", "")
            return CommandResult(0, "", "")

        results = b.ensure_databricks_notebooks("DEFAULT", d, runner=runner)
        assert {r.name for r in results} == set(b.DEFAULT_NOTEBOOKS)
        assert all(r.action == "imported" for r in results)

    def test_skips_when_identical(self, tmp_path):
        d = self._make_notebooks(tmp_path, names=("silver_pipeline",))
        local = (d / "silver_pipeline.py").read_text()

        def runner(cmd):
            joined = " ".join(cmd)
            if "workspace get-status" in joined:
                return CommandResult(0, "{}", "")  # exists
            if "workspace export" in joined:
                # Deployed content identical (plus Databricks header).
                return CommandResult(0, "# Databricks notebook source\n" + local, "")
            if "workspace import" in joined:
                raise AssertionError("should not import when identical")
            return CommandResult(0, "", "")

        results = b.ensure_databricks_notebooks(
            "DEFAULT", d, notebooks=("silver_pipeline",), runner=runner)
        assert results[0].action == "unchanged"

    def test_overwrites_when_changed(self, tmp_path):
        d = self._make_notebooks(tmp_path, names=("silver_pipeline",))
        imported = {"n": 0}

        def runner(cmd):
            joined = " ".join(cmd)
            if "workspace get-status" in joined:
                return CommandResult(0, "{}", "")  # exists (and verify ok)
            if "workspace export" in joined:
                return CommandResult(0, "# Databricks notebook source\nDIFFERENT\n", "")
            if "workspace import" in joined:
                imported["n"] += 1
                return CommandResult(0, "", "")
            return CommandResult(0, "", "")

        results = b.ensure_databricks_notebooks(
            "DEFAULT", d, notebooks=("silver_pipeline",), runner=runner)
        assert results[0].action == "updated"
        assert imported["n"] == 1

    def test_verify_failure_raises(self, tmp_path):
        d = self._make_notebooks(tmp_path, names=("silver_pipeline",))

        def runner(cmd):
            joined = " ".join(cmd)
            if "workspace mkdirs" in joined:
                return CommandResult(0, "", "")
            if "workspace get-status" in joined:
                return CommandResult(1, "", "not found")  # absent + verify absent
            if "workspace import" in joined:
                return CommandResult(0, "", "")
            return CommandResult(0, "", "")

        with pytest.raises(RuntimeError, match="not present after import"):
            b.ensure_databricks_notebooks(
                "DEFAULT", d, notebooks=("silver_pipeline",), runner=runner)

    def test_missing_source_file_raises(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        with pytest.raises(FileNotFoundError):
            b.ensure_databricks_notebooks(
                "DEFAULT", d, notebooks=("silver_pipeline",),
                runner=lambda cmd: CommandResult(0, "", ""))

    def test_mkdirs_failure_raises(self, tmp_path):
        d = self._make_notebooks(tmp_path, names=("silver_pipeline",))
        with pytest.raises(RuntimeError, match="workspace dir"):
            b.ensure_databricks_notebooks(
                "DEFAULT", d, notebooks=("silver_pipeline",),
                runner=lambda cmd: CommandResult(1, "", "denied"))
