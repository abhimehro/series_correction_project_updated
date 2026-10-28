"""Discovery contract for the pr-recap pre-push hook."""

import os
import stat
import subprocess  # nosec B404
import tempfile
import unittest
from pathlib import Path

HOOK_SRC = Path(__file__).resolve().parents[1] / "git-hooks" / "pre-push-pr-recap.sh"
REMOVED = "/Users/speedybee/dev/personal-config/scripts/pr-recap"


def _stub(path, marker, tag):
    """Write an executable that records its tag and arguments."""
    path.parent.mkdir(parents=True, exist_ok=True)
    marker_q = str(marker).replace("'", "'\\''")
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' '{tag}' >> '{marker_q}'\n"
        f"printf '%s\\n' \"$1 $2\" >> '{marker_q}'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


class RecapHookTest(unittest.TestCase):
    """Lookup order for the pre-push pr-recap hook."""

    def setUp(self):
        """Copy the hook into an isolated repo root and home."""
        # pylint: disable=consider-using-with
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.hook = self.root / "scripts" / "git-hooks" / "pre-push-pr-recap.sh"
        self.hook.parent.mkdir(parents=True)
        self.hook.write_bytes(HOOK_SRC.read_bytes())
        self.hook.chmod(0o755)
        self.home = self.root / "home"
        self.home.mkdir()
        self.marker = self.root / "marker"

    def _run(self, extra):
        """Run the hook with an isolated home and optional env overrides."""
        env = {
            "HOME": str(self.home),
            "PATH": "/usr/bin:/bin",
            "PR_RECAP_KEY_TIMEOUT": "1",
        }
        env.update(extra)
        if env.pop("TRACE", "") == "1":
            return subprocess.run(  # nosec B603
                ["/usr/bin/bash", "-x", "./scripts/git-hooks/pre-push-pr-recap.sh"],
                cwd=self.root,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
        return subprocess.run(  # nosec B603
            ["/usr/bin/bash", "./scripts/git-hooks/pre-push-pr-recap.sh"],
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def _lines(self):
        """Return marker lines, or empty when no stub ran."""
        if not self.marker.exists():
            return []
        return self.marker.read_text(encoding="utf-8").splitlines()

    def test_no_candidate(self):
        """Exit cleanly when no pr-recap install is present."""
        result = self._run({})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stderr, "")
        self.assertEqual(self._lines(), [])

    def test_scripts_copy(self):
        """A repo scripts/pr-recap is executed."""
        _stub(self.root / "scripts" / "pr-recap", self.marker, "SCRIPTS")
        result = self._run({})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["SCRIPTS", "sync --pre-push"])

    def test_root_wins(self):
        """Repo-root pr-recap beats the scripts copy."""
        _stub(self.root / "pr-recap", self.marker, "ROOT")
        _stub(self.root / "scripts" / "pr-recap", self.marker, "SCRIPTS")
        result = self._run({})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["ROOT", "sync --pre-push"])

    def test_path_wins(self):
        """PATH beats ~/bin."""
        bindir = self.root / "pathbin"
        _stub(bindir / "pr-recap", self.marker, "PATH")
        _stub(self.home / "bin" / "pr-recap", self.marker, "HOME")
        result = self._run({"PATH": f"{bindir}{os.pathsep}/usr/bin:/bin"})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["PATH", "sync --pre-push"])

    def test_home_bin(self):
        """~/bin/pr-recap is used when it is not on PATH."""
        _stub(self.home / "bin" / "pr-recap", self.marker, "HOME")
        result = self._run({})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["HOME", "sync --pre-push"])

    def test_local_bin(self):
        """~/.local/bin/pr-recap is a supported install location."""
        _stub(self.home / ".local" / "bin" / "pr-recap", self.marker, "LOCAL")
        result = self._run({})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["LOCAL", "sync --pre-push"])

    def test_gemini_bin(self):
        """The user-local Gemini CLI install remains a lookup."""
        rel = Path(".gemini") / "antigravity-cli" / "bin" / "pr-recap"
        _stub(self.home / rel, self.marker, "GEMINI")
        result = self._run({})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["GEMINI", "sync --pre-push"])

    def test_override_wins(self):
        """An executable PR_RECAP_BIN wins over the repo copy."""
        override = self.root / "override dir" / "my-recap"
        _stub(override, self.marker, "OVERRIDE")
        _stub(self.root / "pr-recap", self.marker, "ROOT")
        result = self._run({"PR_RECAP_BIN": str(override)})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["OVERRIDE", "sync --pre-push"])

    def test_bad_override(self):
        """A non-executable PR_RECAP_BIN falls through."""
        override = self.root / "not-exec"
        override.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        override.chmod(0o644)
        _stub(self.root / "scripts" / "pr-recap", self.marker, "SCRIPTS")
        result = self._run({"PR_RECAP_BIN": str(override)})
        self.assertEqual(result.returncode, 0)
        self.assertEqual(self._lines(), ["SCRIPTS", "sync --pre-push"])

    def test_bare_name(self):
        """A bare PR_RECAP_BIN runs the file in the cwd."""
        _stub(self.root / "custom-recap", self.marker, "REL")
        result = self._run({"PR_RECAP_BIN": "custom-recap"})
        self.assertEqual(result.returncode, 0)
        self.assertNotIn("command not found", result.stderr)
        self.assertEqual(self._lines(), ["REL", "sync --pre-push"])

    def test_trace_omits_path(self):
        """The execution trace must not probe the removed workstation path."""
        result = self._run({"TRACE": "1"})
        self.assertEqual(result.returncode, 0)
        self.assertNotIn(REMOVED, result.stderr)
        self.assertNotIn(REMOVED, result.stdout)
        self.assertEqual(self._lines(), [])
