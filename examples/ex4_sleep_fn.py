"""Ex 4: Custom Work Acceptance - gate submissions on external conditions."""

import logging
import tempfile
import os

from jobserver import Blocked, Jobserver


def main() -> None:
    jobserver = Jobserver(context="spawn", slots=2)

    # Create a gate file that controls whether work is accepted
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        gate_path = tmp.name
    logging.info("Gate file: %s", gate_path)

    # sleep_fn returns None (proceed) when gate exists, 0.1 (wait) otherwise
    def sleep_fn_gate() -> float:
        if os.path.exists(gate_path):
            return None  # type: ignore[return-value]
        return 0.1

    # Submission proceeds because the gate file exists
    future_a = jobserver.submit(
        fn=task_greet, args=("accepted",), sleep_fn=sleep_fn_gate
    )
    logging.info("With gate file: %s", future_a.result())

    # Remove the gate so sleep_fn keeps returning 0.1 until timeout
    os.unlink(gate_path)
    try:
        jobserver.submit(
            fn=task_greet,
            args=("rejected",),
            sleep_fn=sleep_fn_gate,
            timeout=0.35,
        )
    except Blocked:
        logging.info("Caught expected Blocked: gate file absent")


def task_greet(name: str) -> str:
    """Return a greeting."""
    return "Hello, " + name


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
