#!/usr/bin/env python3
"""Measure per-test-method line coverage and compare suspected duplicate pairs.

Usage:
    uv run python measure_coverage.py

The script runs each nominated test in isolation, combines the parallel
.coverage.* files that multiprocessing creates, loads the covered-line sets
for every source file in src/jobserver, then reports Jaccard similarity
between every pair.

Coverage config (.coveragerc) already sets:
    concurrency = multiprocessing,thread
    parallel    = true
    sigterm     = true
so subprocess coverage is captured automatically.
"""

import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent
SRC  = ROOT / "src" / "jobserver"

# ---------------------------------------------------------------------------
# Tests to measure – (label, dotted-unittest-id) tuples
# ---------------------------------------------------------------------------
TESTS = [
    # ---- test_jobserver_worker: three flavours of worker abnormal exit ----
    ("jw.sysExit",   "test.test_jobserver_worker.TestJobserverWorker.test_base_exception_surfaces_as_submission_died"),
    ("jw.KBD",       "test.test_jobserver_worker.TestJobserverWorker.test_keyboard_interrupt_becomes_submission_died"),
    ("jw.osExit",    "test.test_jobserver_worker.TestJobserverWorker.test_os_exit_becomes_submission_died"),

    # ---- test_jobserver_map: exception at different positions ----
    ("jm.excFirst",  "test.test_jobserver_map.TestJobserverMap.test_exception_at_first"),
    ("jm.excMid",    "test.test_jobserver_map.TestJobserverMap.test_exception_midstream"),
    ("jm.excChunk",  "test.test_jobserver_map.TestJobserverMap.test_exception_with_chunksize"),
    ("jm.excProp",   "test.test_jobserver_map.TestJobserverMap.test_exception_propagates"),

    # ---- test_executor_basic: sys.exit vs SIGKILL ----
    ("eb.sysExit",   "test.test_executor_basic.TestExceptionPropagation.test_sys_exit"),
    ("eb.sigKill",   "test.test_executor_basic.TestExceptionPropagation.test_worker_killed_by_signal"),

    # ---- cross-layer None return ----
    ("jb.noneRet",   "test.test_jobserver_basic.TestJobserverBasic.test_returns_none"),
    ("eb.noneRet",   "test.test_executor_basic.TestSubmitAndResult.test_none_return"),

    # ---- cross-layer exception-returned-not-raised ----
    ("jb.excRet",    "test.test_jobserver_basic.TestJobserverBasic.test_returns_not_raises_exception"),
    ("eb.excRet",    "test.test_executor_basic.TestExceptionPropagation.test_exception_returned_not_raised"),

    # ---- cross-layer heavy usage ----
    ("jb.heavy",     "test.test_jobserver_basic.TestJobserverBasic.test_heavyusage"),
    ("ec.heavy",     "test.test_executor_concurrency.TestConcurrencyStress.test_heavy_submission"),

    # ---- same helper: threaded submit stress ----
    ("ec.threads",   "test.test_executor_concurrency.TestConcurrencyStress.test_concurrent_submit_threads"),
    ("ec.switchInt", "test.test_executor_concurrency.TestConcurrencyStress.test_setswitchinterval_stress"),

    # ---- callback draining at exit ----
    ("jc.reclaimCB", "test.test_jobserver_callbacks.TestJobserverCallbacks.test_reclaim_resources_raises_callback_raised"),
    ("jc.exitDrain", "test.test_jobserver_callbacks.TestJobserverCallbacks.test_exit_drains_callback_raised"),

    # ---- start-methods all-in-one vs individual ----
    ("eb.success",   "test.test_executor_basic.TestSubmitAndResult.test_successful_call"),
    ("ec.allMeth",   "test.test_executor_concurrency.TestStartMethods.test_all_methods"),

    # ---- map: all-methods vs single-method ----
    ("jm.argsOnly",  "test.test_jobserver_map.TestJobserverMap.test_argses_only"),
    ("ec.mapMeth",   "test.test_executor_concurrency.TestStartMethods.test_map_all_methods"),
    ("eb.mapBasic",  "test.test_executor_basic.TestMap.test_basic"),

    # ---- large objects cross-layer ----
    ("jb.large",     "test.test_jobserver_basic.TestJobserverBasic.test_large_objects"),
    ("eb.large",     "test.test_executor_basic.TestExceptionPropagation.test_large_arguments_and_results"),

    # ---- map empty inputs cross-layer ----
    ("jm.empty",     "test.test_jobserver_map.TestJobserverMap.test_empty_inputs"),
    ("eb.mapEmpty",  "test.test_executor_basic.TestMap.test_empty_iterables"),
]

# ---------------------------------------------------------------------------
# Pairs to compare (label-A, label-B, description)
# ---------------------------------------------------------------------------
PAIRS = [
    ("jw.sysExit",   "jw.KBD",       "worker abnormal exit: sys.exit vs KeyboardInterrupt"),
    ("jw.sysExit",   "jw.osExit",    "worker abnormal exit: sys.exit vs os._exit"),
    ("jw.KBD",       "jw.osExit",    "worker abnormal exit: KeyboardInterrupt vs os._exit"),

    ("jm.excFirst",  "jm.excMid",    "map exception: position 0 vs position 2"),
    ("jm.excFirst",  "jm.excChunk",  "map exception: at-first vs with-chunksize"),
    ("jm.excMid",    "jm.excChunk",  "map exception: midstream vs with-chunksize"),
    ("jm.excProp",   "jm.argsOnly",  "map: exception_propagates (no-raise) vs argses_only"),

    ("eb.sysExit",   "eb.sigKill",   "executor: sys.exit vs SIGKILL worker death"),

    ("jb.noneRet",   "eb.noneRet",   "cross-layer: None return via Jobserver vs Executor"),
    ("jb.excRet",    "eb.excRet",    "cross-layer: exception-returned via Jobserver vs Executor"),

    ("jb.heavy",     "ec.heavy",     "cross-layer: heavy usage via Jobserver vs Executor"),

    ("ec.threads",   "ec.switchInt", "executor stress: same helper, setswitchinterval wrapper"),

    ("jc.reclaimCB", "jc.exitDrain", "callbacks: reclaim_resources raises vs __exit__ drains"),

    ("eb.success",   "ec.allMeth",   "executor: single-method success vs all-methods-in-one"),

    ("jm.argsOnly",  "ec.mapMeth",   "map all-methods: Jobserver vs Executor"),
    ("eb.mapBasic",  "ec.mapMeth",   "map all-methods: Executor basic vs Executor all-methods"),

    ("jb.large",     "eb.large",     "cross-layer: large objects via Jobserver vs Executor"),

    ("jm.empty",     "eb.mapEmpty",  "cross-layer: empty map via Jobserver vs Executor"),
]


def run_test_with_coverage(label: str, test_id: str, work_dir: Path) -> dict:
    """Run one test method, combine parallel coverage, return {srcfile: set(lines)}."""
    cov_file = work_dir / f".coverage_{label}"

    # Remove any stale coverage files for this label
    for f in work_dir.glob(f".coverage_{label}*"):
        f.unlink()

    env = os.environ.copy()
    env["COVERAGE_FILE"] = str(cov_file)

    cmd = [
        sys.executable, "-m", "coverage", "run",
        f"--rcfile={ROOT / '.coveragerc'}",
        "-m", "unittest", test_id,
    ]
    result = subprocess.run(
        cmd,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 1):  # 1 = test failure is acceptable for negative tests
        # Print stderr for debugging if unexpected
        pass

    # Combine all parallel .coverage_label.* files
    parallel_files = list(work_dir.glob(f".coverage_{label}*"))
    if not parallel_files:
        print(f"  WARNING: no coverage files found for {label}", file=sys.stderr)
        return {}

    combine_env = os.environ.copy()
    combine_env["COVERAGE_FILE"] = str(cov_file)
    subprocess.run(
        [sys.executable, "-m", "coverage", "combine",
         f"--rcfile={ROOT / '.coveragerc'}",
         "--data-file", str(cov_file),
         ] + [str(f) for f in parallel_files],
        cwd=ROOT,
        env=combine_env,
        capture_output=True,
    )

    # Export to JSON
    json_path = work_dir / f"cov_{label}.json"
    subprocess.run(
        [sys.executable, "-m", "coverage", "json",
         f"--rcfile={ROOT / '.coveragerc'}",
         "--data-file", str(cov_file),
         "-o", str(json_path),
         ],
        cwd=ROOT,
        env=combine_env,
        capture_output=True,
    )

    if not json_path.exists():
        print(f"  WARNING: no JSON for {label}", file=sys.stderr)
        return {}

    with open(json_path) as fh:
        data = json.load(fh)

    # Extract covered lines per source file (relative to src/)
    covered: dict[str, frozenset] = {}
    for filepath, info in data.get("files", {}).items():
        p = Path(filepath)
        # Make absolute if relative (JSON paths may be relative to ROOT)
        if not p.is_absolute():
            p = ROOT / p
        # Only care about jobserver source files
        try:
            rel = p.relative_to(ROOT / "src")
            key = str(rel)
        except ValueError:
            continue
        executed = frozenset(info.get("executed_lines", []))
        if executed:
            covered[key] = executed

    return covered


def jaccard(a: frozenset, b: frozenset) -> float:
    if not a and not b:
        return 1.0
    union = a | b
    if not union:
        return 1.0
    return len(a & b) / len(union)


def coverage_for_pair(cov_a: dict, cov_b: dict):
    """Compute combined Jaccard across all source files."""
    all_files = set(cov_a) | set(cov_b)
    total_union = 0
    total_inter = 0
    per_file = {}
    for f in sorted(all_files):
        a = cov_a.get(f, frozenset())
        b = cov_b.get(f, frozenset())
        inter = a & b
        union = a | b
        total_union += len(union)
        total_inter += len(inter)
        only_a = a - b
        only_b = b - a
        per_file[f] = {
            "both": sorted(inter),
            "only_a": sorted(only_a),
            "only_b": sorted(only_b),
            "jaccard": jaccard(a, b),
        }
    overall = total_inter / total_union if total_union else 1.0
    return overall, per_file


def main():
    work_dir = ROOT  # run in project root so .coveragerc is found

    print("=" * 70)
    print("Measuring per-test-method coverage...")
    print("=" * 70)

    coverage_by_label: dict[str, dict] = {}
    for label, test_id in TESTS:
        print(f"  {label:16s}  {test_id.split('.')[-1]}", flush=True)
        coverage_by_label[label] = run_test_with_coverage(label, test_id, work_dir)

    print()
    print("=" * 70)
    print("Pairwise coverage comparison")
    print("=" * 70)

    results = []
    for label_a, label_b, desc in PAIRS:
        cov_a = coverage_by_label.get(label_a, {})
        cov_b = coverage_by_label.get(label_b, {})
        overall, per_file = coverage_for_pair(cov_a, cov_b)
        results.append((overall, label_a, label_b, desc, per_file))

    # Sort by similarity descending
    results.sort(key=lambda x: -x[0])

    for overall, label_a, label_b, desc, per_file in results:
        print(f"\n{'─'*70}")
        print(f"  {desc}")
        print(f"  {label_a}  vs  {label_b}    overall Jaccard = {overall:.3f}")
        for f, info in per_file.items():
            j = info["jaccard"]
            n_both   = len(info["both"])
            n_only_a = len(info["only_a"])
            n_only_b = len(info["only_b"])
            print(f"    {f}")
            print(f"      both={n_both:3d}  only_{label_a}={n_only_a:3d}  only_{label_b}={n_only_b:3d}  J={j:.3f}")
            if n_only_a:
                print(f"      lines only in {label_a}: {info['only_a'][:30]}")
            if n_only_b:
                print(f"      lines only in {label_b}: {info['only_b'][:30]}")

    # Cleanup coverage artifacts
    print("\n\nCleaning up coverage files...")
    for f in ROOT.glob(".coverage_*"):
        f.unlink(missing_ok=True)
    for f in ROOT.glob("cov_*.json"):
        f.unlink(missing_ok=True)
    for f in ROOT.glob(".coverage.*"):
        f.unlink(missing_ok=True)

    print("Done.")


if __name__ == "__main__":
    main()
