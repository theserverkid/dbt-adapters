#!/usr/bin/env python3
"""Fail if a GitHub Actions job is missing timeout-minutes or sets it above 5.

Reusable-workflow caller jobs (a job-level `uses:` key) cannot take
timeout-minutes. This lint skips those jobs; the called workflow must
define timeout-minutes on its own jobs.

Usage: python3 .github/scripts/check_job_timeouts.py [path ...]
Default: every .github/workflows/*.{yml,yaml} under the repo root.
Does not call GitHub or AWS.

Exit 0 if every runnable job has timeout-minutes in 1..5.
Exit 1 if a workflow is missing, unreadable, or a job is out of policy.
Exit 2 for usage or missing-dependency errors.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("PyYAML is required (pip install pyyaml).", file=sys.stderr)
    sys.exit(2)

LIMIT = 5
WORKFLOW_GLOBS = (".github/workflows/*.yml", ".github/workflows/*.yaml")


def repo_root() -> Path:
    here = Path(__file__).resolve()
    if here.parent.name == "scripts" and here.parent.parent.name == ".github":
        return here.parents[2]
    start = Path.cwd().resolve()
    for cand in (start, *start.parents):
        if (cand / ".git").exists() or (cand / ".github" / "workflows").is_dir():
            return cand
    return start


def workflow_paths(root: Path, argv: list[str]) -> list[Path]:
    if argv:
        paths = [Path(a) for a in argv]
        missing = [p for p in paths if not p.is_file()]
        if missing:
            for p in missing:
                print(f"not found: {p}", file=sys.stderr)
            sys.exit(1)
        return paths
    found: list[Path] = []
    for glob in WORKFLOW_GLOBS:
        found.extend(sorted(root.glob(glob)))
    return found


def as_timeout(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def check_file(path: Path, root: Path) -> list[str]:
    rel = path.as_posix()
    try:
        rel = path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        pass
    try:
        text = path.read_text(encoding="utf-8")
        data = yaml.safe_load(text)
    except Exception as exc:
        return [f"{rel}: cannot parse YAML: {exc}"]

    if data is None:
        return [f"{rel}: file is empty"]
    if not isinstance(data, dict):
        return [f"{rel}: workflow must be a mapping"]

    jobs = data.get("jobs")
    if jobs is None:
        if "on" in data or True in data:
            return [f"{rel}: workflow has no jobs"]
        return []
    if not isinstance(jobs, dict):
        return [f"{rel}: jobs must be a mapping"]

    errors: list[str] = []
    for name, job in jobs.items():
        if job is None:
            errors.append(f"{rel}: job {name!r} is empty")
            continue
        if not isinstance(job, dict):
            errors.append(f"{rel}: job {name!r} must be a mapping")
            continue
        if "uses" in job:
            continue
        raw = job.get("timeout-minutes")
        timeout = as_timeout(raw)
        if raw is None:
            errors.append(
                f"{rel}: job {name!r} has no timeout-minutes "
                f"(must be 1..{LIMIT})"
            )
        elif timeout is None:
            errors.append(
                f"{rel}: job {name!r} has a non-integer timeout-minutes "
                f"({raw!r}); must be 1..{LIMIT}"
            )
        elif timeout < 1 or timeout > LIMIT:
            errors.append(
                f"{rel}: job {name!r} has timeout-minutes={timeout} "
                f"(must be 1..{LIMIT})"
            )
    return errors


def main(argv: list[str]) -> int:
    if argv and argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    root = repo_root()
    paths = workflow_paths(root, argv)
    if not paths:
        print(f"no workflow files found under {root}", file=sys.stderr)
        return 1

    errors: list[str] = []
    checked_jobs = 0
    skipped_callers = 0
    for path in paths:
        errors.extend(check_file(path, root))
        # counts for the summary (best-effort, ignore parse issues already reported)
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            jobs = data.get("jobs") or {}
            if isinstance(jobs, dict):
                for job in jobs.values():
                    if isinstance(job, dict) and "uses" in job:
                        skipped_callers += 1
                    elif isinstance(job, dict):
                        checked_jobs += 1
        except Exception:
            pass

    print(
        f"Checked {len(paths)} workflow file(s), "
        f"{checked_jobs} job(s); skipped {skipped_callers} reusable-workflow caller(s)."
    )
    if errors:
        for line in errors:
            print(line, file=sys.stderr)
            print(f"::error::{line}", file=sys.stderr)
        print(
            f"{len(errors)} job(s) missing timeout-minutes or over {LIMIT} minutes.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
