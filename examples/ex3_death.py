"""Ex 3: Process Death Detection - detect when workers die unexpectedly."""

import logging
import os
import signal

from jobserver import Jobserver, SubmissionDied


def main() -> None:
    jobserver = Jobserver(context="forkserver", slots=2)

    # Submit work that will die from various signals
    future_killed = jobserver.submit(
        fn=task_self_signal, args=(signal.SIGKILL,)
    )
    future_termed = jobserver.submit(
        fn=task_self_signal, args=(signal.SIGTERM,)
    )

    # Submit normal work alongside the doomed submissions
    future_ok = jobserver.submit(fn=len, args=("hello",))

    # done() returns True even for dead submissions
    logging.info("Killed worker done: %s", future_killed.done())
    logging.info("Normal worker done: %s", future_ok.done())
    logging.info("Normal result: %s", future_ok.result())

    # result() raises SubmissionDied for killed workers
    for name, future in [
        ("SIGKILL", future_killed),
        ("SIGTERM", future_termed),
    ]:
        try:
            future.result()
        except SubmissionDied:
            logging.info("Caught expected SubmissionDied from %s worker", name)


def task_self_signal(sig: signal.Signals) -> None:
    """Send the given signal to the current process."""
    os.kill(os.getpid(), sig)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
