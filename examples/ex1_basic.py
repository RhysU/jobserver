"""Ex 1: Basic Parallel Processing - submit jobs and collect results."""

import logging

from jobserver import Jobserver


def main() -> None:
    jobserver = Jobserver(context="spawn", slots=2)

    # Shorthand with positional args
    future_a = jobserver(len, (1, 2, 3))

    # Shorthand with keyword args
    future_b = jobserver(str, object=42)

    # Full form with args and kwargs
    future_c = jobserver.submit(fn=pow, args=(2, 10), kwargs={"mod": 1000})

    # Full form with a user-defined task function
    future_d = jobserver.submit(fn=task_sum, args=([10, 20, 30],))

    # Results retrieved in arbitrary order
    logging.info("pow(2, 10, mod=1000) = %s", future_c.result())
    logging.info("task_sum([10, 20, 30]) = %s", future_d.result())
    logging.info("len((1, 2, 3)) = %s", future_a.result())
    logging.info("str(object=42) = %s", future_b.result())


def task_sum(numbers: list) -> int:
    """Return the sum of numbers."""
    return sum(numbers)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
