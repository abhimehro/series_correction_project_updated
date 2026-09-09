# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

CLI tool to detect and correct discontinuities (jumps, gaps, outliers) in Seatek
sensor time-series data. Outputs corrected Excel files. No frontend, no
database, no Docker required. See `README.md` for full details.

### Key commands

| Task                 | Command                                                                             |
| -------------------- | ----------------------------------------------------------------------------------- |
| Install deps         | `pip3 install -r scripts/requirements-dev.txt && pip3 install -e .`                 |
| Run tests            | `python3 -m pytest scripts/tests/ -v`                                               |
| Run tests + coverage | `python3 -m pytest --cov=scripts scripts/tests/ -v`                                 |
| Lint                 | `flake8 scripts/ --max-line-length=100`                                             |
| CLI help             | `seatek-correction --help`                                                          |
| CLI dry-run          | `seatek-correction --series 26 --river-miles 54.0 53.0 --years 1995 1996 --dry-run` |
| Batch processing     | `python3 scripts/manual_batch_run.py`                                               |

### Non-obvious caveats

- **Runtime dependencies are pinned**: `scripts/requirements.txt` contains exact
  `==` pins for `pandas`, `numpy`, and `openpyxl`. Dev/test tools live in
  `scripts/requirements-dev.txt` and are not included in the package's runtime
  `install_requires`.
- **Mocks**: `scripts/tests/test_batch_correction.py` uses `unittest.mock`
  (stdlib). No third-party `mock` package is required.
- **Test status**: The full suite passes with
  `python3 -m pytest scripts/tests/ -v`.
- **Test path**: Primary suite is under `scripts/tests/`, not root `tests/`
  (root `tests/` only covers scaffolding helpers).
- **Data files**: `.txt` sensor data files in `data/` are committed for Series
  26 and 27. Output goes to `data/output/`.
- Use `python3` (not `python`) as the command.

### CodeScene review/salvage trigger

During automated PR review or salvage sessions, if CodeScene is red on a PR,
post:

```bash
/cs-agent skill:fix-code-health-degradations
```

Then wait for that run to complete before final defer/salvage disposition.

## Learned User Preferences

- Do not commit local editable-install build metadata when finishing unrelated
  sessions.

## Learned Workspace Facts

- Prefer not staging `seatek_series_correction.egg-info/` or other
  editable-install metadata; keep `*.egg-info/` gitignored.

<!-- gitnexus:start -->

# GitNexus — Code Intelligence

This project is indexed by GitNexus as **series_correction_project_updated**
(717 symbols, 1471 relationships, 53 execution flows).

> Index stale? Run `node .gitnexus/run.cjs analyze --index-only` from the
> project root — it auto-selects an available runner. No `.gitnexus/run.cjs`
> yet? Bootstrap with `npx`, `bunx`, or `pnpm dlx` — e.g.
> `bunx gitnexus@latest analyze` (npm 11 npx crash; #1939).

## Always Do

- **MUST run impact before editing.** Use
  `impact({target: "symbolName", direction: "upstream"})` or
  `node .gitnexus/run.cjs impact "symbolName" --direction upstream --repo .`;
  report callers, processes, and risk. Never substitute grep for graph analysis.
- **MUST analyze graph changes before committing.** Use
  `detect_changes({scope: "all"})` (MCP) or
  `node .gitnexus/run.cjs detect-changes --scope all --repo .` (CLI fallback).
  `partial: true` or `truncated: true` is not a clean check — a zero means
  unseen, not unaffected; re-run it. For regression review:
  `detect_changes({scope: "compare", base_ref: "main"})` or
  `node .gitnexus/run.cjs detect-changes --scope compare --base-ref "main" --repo .`.
- MUST warn on HIGH/CRITICAL `risk` pre-edit; never use `riskSharedAxes` to
  waive a HIGH/CRITICAL `risk` warning. Compare File/symbol: MCP File omits
  axes; Graph-RAG expands File.
- **MUST treat `risk: UNKNOWN` as unresolved, not as low.** An empty caller set
  is not evidence the symbol is unused — it can also mean the callers are not
  resolvable by the index (plain-object property access, dynamic dispatch,
  cross-language calls). `impact` pairs `UNKNOWN` with a `riskNote` saying so.
  Confirm with a text search before treating the symbol as safe to change or
  delete; do not proceed on the strength of a zero.
- **MUST use `query({search_query: "concept"})` for concepts/flows,
  `context({name: "symbolName"})` for a named symbol, or `impact` for blast
  radius, on read-only callers, dependencies, imports, or execution flow.**
  Graph first; text search only for empty/`UNKNOWN`/literals.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings
  (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method before MCP/CLI impact analysis.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis, and never
  read `UNKNOWN` as an all-clear — it means the walk could not answer, which is
  the one verdict that requires confirming by other means.
- NEVER rename symbols with find-and-replace — use `rename` which understands
  the call graph.
- NEVER commit before MCP/CLI graph change analysis.

## Resources

| Resource                                                           | Use for                                  |
| ------------------------------------------------------------------ | ---------------------------------------- |
| `gitnexus://repo/series_correction_project_updated/context`        | Codebase overview, check index freshness |
| `gitnexus://repo/series_correction_project_updated/clusters`       | All functional areas                     |
| `gitnexus://repo/series_correction_project_updated/processes`      | All execution flows                      |
| `gitnexus://repo/series_correction_project_updated/process/{name}` | Step-by-step execution trace             |

## CLI

| Task                                         | Read this skill file                               |
| -------------------------------------------- | -------------------------------------------------- |
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus-exploring/SKILL.md`       |
| Blast radius / "What breaks if I change X?"  | `.claude/skills/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?"             | `.claude/skills/gitnexus-debugging/SKILL.md`       |
| Rename / extract / split / refactor          | `.claude/skills/gitnexus-refactoring/SKILL.md`     |
| Tools, resources, schema reference           | `.claude/skills/gitnexus-guide/SKILL.md`           |
| Index, status, clean, wiki CLI commands      | `.claude/skills/gitnexus-cli/SKILL.md`             |
| Work in the Tests area (170 symbols)         | `.claude/skills/gitnexus-area-tests/SKILL.md`      |
| Work in the Scripts area (101 symbols)       | `.claude/skills/gitnexus-area-scripts/SKILL.md`    |

<!-- gitnexus:end -->
