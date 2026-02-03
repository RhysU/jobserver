"""Ex 5: Callbacks and Exception Handling - when_done and CallbackRaised."""

import logging

from jobserver import CallbackRaised, Jobserver


def main() -> None:
    jobserver = Jobserver(context="fork", slots=2)

    # Register callbacks that fire when the future completes
    results = []  # type: list
    future = jobserver.submit(fn=len, args=("hello",))
    future.when_done(callback_record, results, "first")
    future.when_done(callback_record, results, "second")
    logging.info("Result: %s", future.result())
    logging.info("Callbacks fired: %s", results)

    # Register a callback after the future already has a result.
    # It fires immediately.
    future.when_done(callback_record, results, "after-done")
    logging.info("Callbacks after done: %s", results)

    # When a callback raises, CallbackRaised wraps the original exception.
    # Re-calling done() drains the remaining callbacks one by one.
    future_err = jobserver.submit(fn=len, args=("world",))
    future_err.when_done(callback_raise, ValueError, "bad")
    future_err.when_done(callback_raise, TypeError, "worse")
    future_err.when_done(callback_record, results, "survivor")

    for i in range(3):
        try:
            future_err.done()
            logging.info("done() call %d: no more errors", i)
            break
        except CallbackRaised as e:
            logging.info(
                "done() call %d: caught %s",
                i,
                type(e.__cause__).__name__,
            )

    # The result is still available after all callbacks drain
    logging.info("Result after errors: %s", future_err.result())


def callback_record(results: list, tag: str) -> None:
    """Append a tag to the results list."""
    results.append(tag)


def callback_raise(klass: type, *args) -> None:
    """Raise the requested exception."""
    raise klass(*args)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()
