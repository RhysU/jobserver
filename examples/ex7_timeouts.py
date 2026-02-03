"""Ex 7: Non-blocking Timeouts - polling, deadlines, and Blocked."""

import logging
import time

from jobserver import Blocked, Jobserver, MinimalQueue


def main() -> None:
    jobserver = Jobserver(context="spawn", slots=2)

    # Use a MinimalQueue to create a stalled worker we can unblock later
    mq = MinimalQueue(context="spawn")  # type: MinimalQueue[str]
    future_stalled = jobserver.submit(fn=task_wait, args=(mq,))

    # Zero timeout: done() returns False, result() raises Blocked
    logging.info("done(timeout=0): %s", future_stalled.done(timeout=0))
    try:
        future_stalled.result(timeout=0)
    except Blocked:
        logging.info("Caught expected Blocked from result(timeout=0)")

    # Finite timeout: waits up to the deadline then gives up
    start = time.monotonic()
    try:
        future_stalled.result(timeout=0.25)
    except Blocked:
        elapsed = time.monotonic() - start
        logging.info(
            "Caught expected Blocked from result(timeout=0.25) " "after %.2fs",
            elapsed,
        )

    # Fill the second slot with another stalled worker
    mq_fill = MinimalQueue(context="spawn")  # type: MinimalQueue[str]
    future_fill = jobserver.submit(fn=task_wait, args=(mq_fill,))

    # submit() with timeout=0 raises Blocked when all slots are occupied
    try:
        jobserver.submit(fn=len, args=("rejected",), timeout=0)
    except Blocked:
        logging.info("Caught expected Blocked: no slots for new work")

    # submit() with a finite timeout waits then raises Blocked
    start = time.monotonic()
    try:
        jobserver.submit(fn=len, args=("rejected",), timeout=0.25)
    except Blocked:
        elapsed = time.monotonic() - start
        logging.info(
            "Caught expected Blocked from submit(timeout=0.25) " "after %.2fs",
            elapsed,
        )

    # Unblock both stalled workers and confirm their results
    mq_fill.put("filler")
    mq.put("released")
    logging.info("Filler result: %s", future_fill.result())
    logging.info("Stalled result after unblock: %s", future_stalled.result())


def task_wait(mq: MinimalQueue) -> str:
    """Block until a message arrives on the queue."""
    return mq.get(timeout=60.0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
