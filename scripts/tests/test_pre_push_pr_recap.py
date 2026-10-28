"""Discovery contract for the pr-recap pre-push hook."""

import os
import stat
import subprocess
from pathlib import Path

import pytest

HOOK_SRC = Path(__file__).resolve().parents[1] / "git-hooks" / "pre-push-pr-recap.sh"
PLANTED = Path("/Users/speedybee/dev/personal-config/scripts/pr-recap")
BASH = "/usr/bin/bash"


def _stub(path, marker, tag):
    path.parent.mkdir(parents=True, exist_ok=True)
    marker_q = str(marker).replace("'", "'\\''")
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' '{tag}' >> '{marker_q}'\n"
        f"printf '%s\\n' \"$1 $2\" >> '{marker_q}'\n"
        "exit 0\n"
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _layout(tmp_path):
    hook = tmp_path / "scripts" / "git-hooks" / "pre-push-pr-recap.sh"
    hook.parent.mkdir(parents=True)
    hook.write_bytes(HOOK_SRC.read_bytes())
    hook.chmod(0o755)
    home = tmp_path / "home"
    home.mkdir()
    return hook, home, tmp_path / "marker"


def _run(hook, cwd, home, extra=None, trace=False):
    env = {
        "HOME": str(home),
        "PATH": "/usr/bin:/bin",
        "PR_RECAP_KEY_TIMEOUT": "1",
    }
    if extra:
        env.update(extra)
    argv = [BASH, "-x", str(hook)] if trace else [BASH, str(hook)]
    return subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _lines(marker):
    if not marker.exists():
        return []
    return marker.read_text().splitlines()


def _remove_empty_parents(path):
    parent = path
    for _ in range(8):
        try:
            parent.rmdir()
        except OSError:
            return
        parent = parent.parent


@pytest.fixture
def planted(tmp_path):
    """Executable at the removed machine-specific path, cleaned up after."""
    marker = tmp_path / "planted-marker"
    if PLANTED.exists():
        pytest.skip(f"refusing to replace existing {PLANTED}")
    try:
        PLANTED.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        pytest.skip(f"cannot plant {PLANTED}: {exc}")
    _stub(PLANTED, marker, "PLANTED")
    try:
        yield marker
    finally:
        PLANTED.unlink(missing_ok=True)
        _remove_empty_parents(PLANTED.parent)


def test_planted_personal_path_is_not_executed(tmp_path, planted):
    hook, home, marker = _layout(tmp_path)
    result = _run(hook, tmp_path, home)
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == []
    assert _lines(planted) == []
    assert "speedybee" not in result.stderr


def test_supported_lookup_wins_over_planted_path(tmp_path, planted):
    hook, home, marker = _layout(tmp_path)
    _stub(tmp_path / "scripts" / "pr-recap", marker, "SCRIPTS")
    result = _run(hook, tmp_path, home)
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == ["SCRIPTS", "sync --pre-push"]
    assert _lines(planted) == []


def test_no_candidate_exits_without_running_a_binary(tmp_path):
    hook, home, marker = _layout(tmp_path)
    result = _run(hook, tmp_path, home)
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == []
    assert result.stderr == ""


@pytest.mark.parametrize(
    ("relpath", "home_rel", "path_dir"),
    [
        ("pr-recap", None, False),
        ("scripts/pr-recap", None, False),
        (None, "bin/pr-recap", False),
        (None, ".local/bin/pr-recap", False),
        (None, ".gemini/antigravity-cli/bin/pr-recap", False),
        (None, None, True),
    ],
)
def test_supported_install_locations(tmp_path, relpath, home_rel, path_dir):
    hook, home, marker = _layout(tmp_path)
    extra = {}
    if relpath is not None:
        _stub(tmp_path / relpath, marker, "FOUND")
    if home_rel is not None:
        _stub(home / home_rel, marker, "FOUND")
    if path_dir:
        bindir = tmp_path / "pathbin"
        _stub(bindir / "pr-recap", marker, "FOUND")
        extra["PATH"] = f"{bindir}{os.pathsep}/usr/bin:/bin"
    result = _run(hook, tmp_path, home, extra)
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == ["FOUND", "sync --pre-push"]


def test_repo_root_beats_scripts_copy(tmp_path):
    hook, home, marker = _layout(tmp_path)
    _stub(tmp_path / "pr-recap", marker, "ROOT")
    _stub(tmp_path / "scripts" / "pr-recap", marker, "SCRIPTS")
    result = _run(hook, tmp_path, home)
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == ["ROOT", "sync --pre-push"]


def test_path_beats_home_bin(tmp_path):
    hook, home, marker = _layout(tmp_path)
    bindir = tmp_path / "pathbin"
    _stub(bindir / "pr-recap", marker, "PATH")
    _stub(home / "bin" / "pr-recap", marker, "HOME")
    result = _run(
        hook,
        tmp_path,
        home,
        {"PATH": f"{bindir}{os.pathsep}/usr/bin:/bin"},
    )
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == ["PATH", "sync --pre-push"]


def test_pr_recap_bin_wins_when_executable(tmp_path):
    hook, home, marker = _layout(tmp_path)
    override = tmp_path / "override dir" / "my-recap"
    _stub(override, marker, "OVERRIDE")
    _stub(tmp_path / "pr-recap", marker, "ROOT")
    result = _run(hook, tmp_path, home, {"PR_RECAP_BIN": str(override)})
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == ["OVERRIDE", "sync --pre-push"]


def test_non_executable_override_falls_through(tmp_path):
    hook, home, marker = _layout(tmp_path)
    override = tmp_path / "not-exec"
    override.write_text("#!/bin/sh\nexit 0\n")
    override.chmod(0o644)
    _stub(tmp_path / "scripts" / "pr-recap", marker, "SCRIPTS")
    result = _run(hook, tmp_path, home, {"PR_RECAP_BIN": str(override)})
    assert result.returncode == 0, result.stderr
    assert _lines(marker) == ["SCRIPTS", "sync --pre-push"]


def test_bare_relative_override_runs_the_cwd_file(tmp_path):
    hook, home, marker = _layout(tmp_path)
    _stub(tmp_path / "custom-recap", marker, "REL")
    result = _run(hook, tmp_path, home, {"PR_RECAP_BIN": "custom-recap"})
    assert result.returncode == 0, result.stderr
    assert "command not found" not in result.stderr
    assert _lines(marker) == ["REL", "sync --pre-push"]


def test_trace_never_probes_machine_specific_path(tmp_path):
    """CI cannot plant /Users; the execution trace must still omit that probe."""
    hook, home, marker = _layout(tmp_path)
    result = _run(hook, tmp_path, home, trace=True)
    assert result.returncode == 0, result.stderr
    assert str(PLANTED) not in result.stderr
    assert str(PLANTED) not in result.stdout
    assert _lines(marker) == []
