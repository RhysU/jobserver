# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""Benchmark harness exercising Jobserver and JobserverExecutor.

Runs a handful of small, semi-realistic, CPU-bound workflows and prints
aligned tables of wall time, throughput, and speedup versus a serial
baseline.  Where meaningful, the standard library's ProcessPoolExecutor
is measured side-by-side to quantify the "slower launch, more robust"
tradeoff that JobserverExecutor deliberately makes.

To keep timings stable rather than noisy, every configuration (including
the serial baseline) is measured with the same discipline: a few untimed
warmup iterations first prime imports, the page cache, and fork/spawn
machinery; garbage collection is paused across each timed region so a
stray collection cannot inflate a single sample; and the best (smallest)
of several repeated measurements is reported, since the minimum is the
sample least perturbed by transient system load.  Tune with --warmup and
--repeat.

All kernels are module-level functions (hence picklable under spawn and
forkserver), deterministic, and stdlib-only.  Run, for example, with:

    PYTHONPATH=src python draft/bench.py --quick
    PYTHONPATH=src python draft/bench.py --context spawn --slots 4
    PYTHONPATH=src python draft/bench.py --only flat,nested,executor

The six scenarios are:

  flat      Jobserver.map() map-reduce throughput across slots/chunksize.
  nested    Recursive divide-and-conquer sharing one shared slot pool,
            the nestable workflow on which a fixed-size pool deadlocks.
  callbacks when_done() partial-sum callbacks drained via eager reclaim.
  sleepfn   Admission-control overhead introduced by a sleep_fn gate.
  executor  JobserverExecutor vs ProcessPoolExecutor on map and submit.
  cancel    Cancellation churn against genuinely PENDING futures.
"""

import argparse
import gc
import os
import platform
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from multiprocessing import get_context
from typing import Optional

from jobserver import Blocked, Jobserver, JobserverExecutor

# ---------------------------------------------------------------------------
# CPU-bound kernels.  Kept trivial, deterministic, and importable so that
# they survive pickling to spawn/forkserver children.
# ---------------------------------------------------------------------------


def _is_prime(n: int) -> bool:
    """Return whether n is prime via bounded trial division."""
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True


def count_primes(lo: int, hi: int) -> int:
    """Count primes in the half-open range [lo, hi)."""
    return sum(1 for n in range(max(lo, 2), hi) if _is_prime(n))


def nested_count(
    jobserver: Jobserver, lo: int, hi: int, threshold: int
) -> int:
    """Recursively halve [lo, hi), offloading halves to the same pool.

    Each call tries to submit its left half back to the shared jobserver
    with timeout=0; when no slot is free it falls back to computing both
    halves inline.  This graceful degradation is exactly why a nestable
    token pool does not deadlock where a fixed-size pool would.
    """
    if hi - lo <= threshold:
        return count_primes(lo, hi)
    mid = (lo + hi) // 2
    try:
        future = jobserver.submit(
            fn=nested_count,
            args=(jobserver, lo, mid, threshold),
            timeout=0,
        )
    except Blocked:
        left = nested_count(jobserver, lo, mid, threshold)
        return left + nested_count(jobserver, mid, hi, threshold)
    right = nested_count(jobserver, mid, hi, threshold)
    return future.result() + right


def admit_always() -> Optional[float]:
    """A sleep_fn that always admits work yet still costs a call/pickle."""
    return None


# ---------------------------------------------------------------------------
# Tiny timing and reporting helpers.
# ---------------------------------------------------------------------------


@dataclass
class Row:
    """One measured configuration within a scenario's results table."""

    label: str
    wall: float
    tasks: int
    speedup: float
    note: str = ""


@dataclass
class Workload:
    """Immutable description of the shared map-reduce workload."""

    n: int
    parts: int
    slots: int
    context: Optional[str]
    threshold: int
    warmup: int = 1
    repeat: int = 5
    serial_wall: float = 0.0
    serial_total: int = 0
    ranges: list = field(default_factory=list)

    def jobserver(
        self, slots: Optional[int] = None, **kw: object
    ) -> Jobserver:
        """Construct a Jobserver honoring the chosen context and slots."""
        return Jobserver(
            context=self.context,
            slots=self.slots if slots is None else slots,
            **kw,  # type: ignore[arg-type]
        )


def partition(n: int, parts: int) -> list:
    """Split [2, n) into parts contiguous (lo, hi) ranges."""
    edges = [2 + (n - 2) * i // parts for i in range(parts + 1)]
    return [(edges[i], edges[i + 1]) for i in range(parts)]


class _Stopwatch:
    """Holds the elapsed seconds populated when a stopwatch() block exits."""

    elapsed: float = 0.0


@contextmanager
def stopwatch() -> Iterator[_Stopwatch]:
    """Time a block on the monotonic clock with garbage collection paused.

    Collection is disabled across the timed region so a stray collection
    cannot inflate a single measurement, then restored to its prior state.
    Read the duration from the yielded object's .elapsed after the block.
    """
    watch = _Stopwatch()
    gc_was_enabled = gc.isenabled()
    gc.disable()
    start = time.perf_counter()
    try:
        yield watch
    finally:
        watch.elapsed = time.perf_counter() - start
        if gc_was_enabled:
            gc.enable()


def measure_best(w: "Workload", trial: Callable[[], Row]) -> Row:
    """Warm up, then repeat trial, returning the Row with least wall time.

    trial runs one full timed iteration of a single configuration and
    returns its Row.  w.warmup untimed iterations first prime imports, the
    page cache, and fork/spawn machinery; the best (smallest wall) of the
    subsequent w.repeat measured iterations is kept, since the minimum is
    the measurement least perturbed by transient system load.
    """
    for _ in range(w.warmup):
        trial()
    best: Optional[Row] = None
    for _ in range(max(1, w.repeat)):
        row = trial()
        if best is None or row.wall < best.wall:
            best = row
    assert best is not None
    return best


def print_table(title: str, rows: list) -> None:
    """Print an aligned results table for one scenario."""
    print(f"\n== {title} ==")
    head = ("config", "wall(s)", "tasks/s", "speedup", "note")
    widths = [len(h) for h in head]
    cells = []
    for r in rows:
        rate = r.tasks / r.wall if r.wall > 0 else 0.0
        cell = (
            r.label,
            f"{r.wall:.3f}",
            f"{rate:,.0f}",
            f"{r.speedup:.2f}x",
            r.note,
        )
        cells.append(cell)
        widths = [max(w, len(c)) for w, c in zip(widths, cell)]

    def fmt(cols: tuple) -> str:
        return "  ".join(c.ljust(w) for c, w in zip(cols, widths))

    print(fmt(head))
    print("  ".join("-" * w for w in widths))
    for cell in cells:
        print(fmt(cell))


# ---------------------------------------------------------------------------
# Scenarios.  Each takes the shared Workload and returns its table rows.
# ---------------------------------------------------------------------------


def scenario_flat(w: Workload) -> list:
    """Jobserver.map() throughput across slot counts and chunksizes."""
    rows = []
    for slots in sorted({1, 2, w.slots}):
        for chunksize in (1, max(1, w.parts // slots)):

            def trial(slots: int = slots, chunksize: int = chunksize) -> Row:
                with w.jobserver(slots=slots) as js:
                    with stopwatch() as sw:
                        total = sum(
                            js.map(
                                fn=count_primes,
                                argses=w.ranges,
                                chunksize=chunksize,
                            )
                        )
                assert total == w.serial_total, (total, w.serial_total)
                return Row(
                    label=f"slots={slots} chunk={chunksize}",
                    wall=sw.elapsed,
                    tasks=w.parts,
                    speedup=w.serial_wall / sw.elapsed if sw.elapsed else 0.0,
                )

            rows.append(measure_best(w, trial))
    return rows


def scenario_nested(w: Workload) -> list:
    """Recursive divide-and-conquer sharing a single slot pool."""
    rows = []
    leaves = max(1, (w.n - 2 + w.threshold - 1) // w.threshold)
    for slots in sorted({1, 2, w.slots}):

        def trial(slots: int = slots) -> Row:
            with w.jobserver(slots=slots) as js:
                with stopwatch() as sw:
                    total = js.submit(
                        fn=nested_count,
                        args=(js, 2, w.n, w.threshold),
                    ).result()
            assert total == w.serial_total, (total, w.serial_total)
            return Row(
                label=f"slots={slots}",
                wall=sw.elapsed,
                tasks=leaves,
                speedup=w.serial_wall / sw.elapsed if sw.elapsed else 0.0,
                note="recurses + degrades; no deadlock",
            )

        rows.append(measure_best(w, trial))
    return rows


def _collect(sink: list, future: object) -> None:
    """when_done() callback recording a finished chunk's partial sum."""
    sink.append(future.result())  # type: ignore[attr-defined]


def scenario_callbacks(w: Workload) -> list:
    """when_done() partial sums drained via eager reclaim_resources()."""

    def trial() -> Row:
        sink: list = []
        with w.jobserver() as js:
            with stopwatch() as sw:
                futures = [
                    js.submit(fn=count_primes, args=r) for r in w.ranges
                ]
                for future in futures:
                    future.when_done(_collect, sink, future)
                # Eagerly reclaim until every chunk reports its partial sum.
                while len(sink) < len(futures):
                    js.reclaim_resources()
        assert sum(sink) == w.serial_total, (sum(sink), w.serial_total)
        return Row(
            label=f"slots={w.slots}",
            wall=sw.elapsed,
            tasks=w.parts,
            speedup=w.serial_wall / sw.elapsed if sw.elapsed else 0.0,
            note=f"{len(sink)} callbacks fired",
        )

    return [measure_best(w, trial)]


def scenario_sleepfn(w: Workload) -> list:
    """Overhead of an always-admitting sleep admission gate."""
    rows = []
    for gated, label in ((False, "no gate"), (True, "sleep")):

        def trial(gated: bool = gated, label: str = label) -> Row:
            with w.jobserver() as js:
                runner = js.replace_sleep(admit_always) if gated else js
                with stopwatch() as sw:
                    total = sum(runner.map(fn=count_primes, argses=w.ranges))
            assert total == w.serial_total, (total, w.serial_total)
            return Row(
                label=label,
                wall=sw.elapsed,
                tasks=w.parts,
                speedup=w.serial_wall / sw.elapsed if sw.elapsed else 0.0,
            )

        rows.append(measure_best(w, trial))
    return rows


def scenario_executor(w: Workload) -> list:
    """JobserverExecutor vs ProcessPoolExecutor on map and submit."""
    rows = []
    ctx = get_context(w.context)
    los = [lo for lo, _ in w.ranges]
    his = [hi for _, hi in w.ranges]

    def measure(label: str, make: Callable, note: str) -> None:
        # map(): in-order results
        def map_trial() -> Row:
            with make() as ex:
                with stopwatch() as sw:
                    total = sum(ex.map(count_primes, los, his))
            assert total == w.serial_total, (total, w.serial_total)
            return Row(
                label=f"{label} map",
                wall=sw.elapsed,
                tasks=w.parts,
                speedup=w.serial_wall / sw.elapsed if sw.elapsed else 0.0,
                note=note,
            )

        # submit() + as_completed(): out-of-order fan-in
        def submit_trial() -> Row:
            with make() as ex:
                with stopwatch() as sw:
                    futures = [
                        ex.submit(count_primes, lo, hi) for lo, hi in w.ranges
                    ]
                    total = sum(f.result() for f in as_completed(futures))
            assert total == w.serial_total, (total, w.serial_total)
            return Row(
                label=f"{label} submit",
                wall=sw.elapsed,
                tasks=w.parts,
                speedup=w.serial_wall / sw.elapsed if sw.elapsed else 0.0,
                note=note,
            )

        rows.append(measure_best(w, map_trial))
        rows.append(measure_best(w, submit_trial))

    measure(
        "Jobserver",
        lambda: JobserverExecutor(w.jobserver()),
        "PENDING-cancellable, robust",
    )
    measure(
        "ProcessPool",
        lambda: ProcessPoolExecutor(max_workers=w.slots, mp_context=ctx),
        "stdlib baseline",
    )
    return rows


def scenario_cancel(w: Workload) -> list:
    """Cancellation churn against genuinely PENDING futures (slots=1)."""
    submitted = max(8, w.parts)

    def trial() -> Row:
        with JobserverExecutor(w.jobserver(slots=1)) as ex:
            with stopwatch() as sw:
                # One slow task holds the only slot; the rest queue PENDING.
                held = ex.submit(time.sleep, 0.2)
                pending = [
                    ex.submit(time.sleep, 5.0) for _ in range(submitted)
                ]
                cancelled = sum(1 for f in pending if f.cancel())
                held.result()
        return Row(
            label=f"submitted={submitted}",
            wall=sw.elapsed,
            tasks=submitted,
            speedup=0.0,
            note=f"{cancelled}/{submitted} cancelled while PENDING",
        )

    return [measure_best(w, trial)]


SCENARIOS: dict = {
    "flat": scenario_flat,
    "nested": scenario_nested,
    "callbacks": scenario_callbacks,
    "sleepfn": scenario_sleepfn,
    "executor": scenario_executor,
    "cancel": scenario_cancel,
}


# ---------------------------------------------------------------------------
# Driver.
# ---------------------------------------------------------------------------


def build_workload(args: argparse.Namespace) -> Workload:
    """Assemble the shared workload and measure the serial baseline.

    The baseline is warmed and taken best-of-repeat just like the
    scenarios, so the speedup ratios that divide by it stay stable.
    """
    w = Workload(
        n=args.n,
        parts=args.parts,
        slots=args.slots,
        context=args.context,
        threshold=max(1, (args.n - 2) // (args.slots * 4)),
        warmup=args.warmup,
        repeat=args.repeat,
    )
    w.ranges = partition(w.n, w.parts)

    def serial() -> int:
        return sum(count_primes(lo, hi) for lo, hi in w.ranges)

    for _ in range(w.warmup):
        serial()
    best = None
    for _ in range(max(1, w.repeat)):
        with stopwatch() as sw:
            w.serial_total = serial()
        best = sw.elapsed if best is None else min(best, sw.elapsed)
    w.serial_wall = best or 0.0
    return w


def main() -> None:
    """Parse arguments, build the workload, and run chosen scenarios."""
    cpus = len(os.sched_getaffinity(0))
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, help="prime sieve upper bound")
    parser.add_argument("--parts", type=int, help="number of work chunks")
    parser.add_argument(
        "--slots", type=int, default=min(4, cpus), help="jobserver slots"
    )
    parser.add_argument(
        "--context",
        choices=("fork", "spawn", "forkserver"),
        default=None,
        help="multiprocessing start method (default: platform default)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="small inputs for a fast smoke run",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="untimed warmup iterations per configuration (default: 1)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=5,
        help="timed iterations per configuration; best is kept (default: 5)",
    )
    parser.add_argument(
        "--only",
        default="",
        help=f"comma-separated subset of {','.join(SCENARIOS)}",
    )
    args = parser.parse_args()

    if args.warmup < 0:
        parser.error("--warmup must be >= 0")
    if args.repeat < 1:
        parser.error("--repeat must be >= 1")

    # Defaults depend on --quick unless explicitly overridden.
    if args.n is None:
        args.n = 150_000 if args.quick else 1_500_000
    if args.parts is None:
        args.parts = 16 if args.quick else 48

    chosen = [s for s in (args.only.split(",") if args.only else SCENARIOS)]
    unknown = [s for s in chosen if s not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenario(s): {', '.join(unknown)}")

    w = build_workload(args)
    print("jobserver benchmark")
    print(
        f"python={platform.python_version()} "
        f"platform={platform.system()} "
        f"cpus={cpus} "
        f"context={args.context or get_context().get_start_method()} "
        f"warmup={w.warmup} repeat={w.repeat}"
    )
    print(
        f"n={w.n:,} parts={w.parts} slots={w.slots} "
        f"threshold={w.threshold:,} primes={w.serial_total:,} "
        f"serial={w.serial_wall:.3f}s (best of {w.repeat})"
    )

    for name in chosen:
        print_table(name, SCENARIOS[name](w))


if __name__ == "__main__":
    main()
