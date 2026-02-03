"""Ex 2: Nested Submissions - work submits more work sharing slots."""

import logging

from jobserver import Blocked, Jobserver


def main() -> None:
    # Using slots=2 here; slots=None would use os.sched_getaffinity(0)
    # to match the number of usable CPUs for the current process.
    jobserver = Jobserver(context="fork", slots=2)

    # Parent submits task_recurse which itself submits more work.
    # Recursion depth is bounded by the number of available slots.
    future = jobserver.submit(fn=task_recurse, args=(jobserver, 10), timeout=5)
    depth = future.result()
    logging.info("Reached recursion depth %d with 2 slots", depth)

    # With more slots, deeper recursion is possible
    jobserver_wide = Jobserver(context="fork", slots=4)
    future_wide = jobserver_wide.submit(
        fn=task_recurse, args=(jobserver_wide, 10), timeout=5
    )
    depth_wide = future_wide.result()
    logging.info("Reached recursion depth %d with 4 slots", depth_wide)


def task_recurse(jobserver: Jobserver, max_depth: int) -> int:
    """Submit nested work until either Blocked or max_depth reached."""
    if max_depth < 1:
        return 0
    try:
        future = jobserver.submit(
            fn=task_recurse, args=(jobserver, max_depth - 1), timeout=0
        )
    except Blocked:
        return 0
    return 1 + future.result(timeout=None)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
