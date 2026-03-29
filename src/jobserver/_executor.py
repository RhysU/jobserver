# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""A concurrent.futures.Executor backed by a Jobserver."""

import concurrent.futures
import itertools
import logging
import queue
import threading
from collections import deque
from collections.abc import Callable, Iterator
from typing import Any, Optional, TypeVar

from . import _request, _response
from ._jobserver import (
    Blocked,
    CallbackRaised,
    Future,
    Jobserver,
)
from ._queue import MinimalQueue

__all__ = ("JobserverExecutor",)

_LOG = logging.getLogger(__name__)

T = TypeVar("T")


class JobserverExecutor(concurrent.futures.Executor):
    """A concurrent.futures.Executor that delegates to a Jobserver.

    Decouples submission from dispatch so that returned
    concurrent.futures.Future instances are genuinely PENDING and
    cancellable before execution begins.  A dispatcher process
    manages slot acquisition and worker spawning via the Jobserver,
    while a thin receiver thread bridges results back to
    concurrent.futures.Future instances.
    """

    def __init__(self, jobserver: Jobserver) -> None:
        """Initialize the executor with a configured Jobserver.

        Slot count, start method, and other execution parameters are
        controlled by the Jobserver instance passed here.
        """
        # One lock guards _shutdown, _work_ids, and _futures together.
        self._lock = threading.Lock()
        self._shutdown = False
        self._work_ids: Iterator[int] = itertools.count()
        self._futures: dict[int, concurrent.futures.Future] = {}

        self._requests: MinimalQueue = MinimalQueue(jobserver.context)
        self._responses: MinimalQueue = MinimalQueue(jobserver.context)

        self._dispatcher = jobserver.context.Process(  # type: ignore
            target=_dispatch_loop,
            args=(jobserver, self._requests, self._responses),
            daemon=False,
            name="JobserverExecutor-dispatcher",
        )
        self._dispatcher.start()

        self._receiver = threading.Thread(
            target=self._receive_loop,
            daemon=True,
            name="JobserverExecutor-receiver",
        )
        self._receiver.start()

        # Close unused pipe ends so EOF propagates on crash.
        # Parent only writes to requests and reads responses.
        self._requests.close_get()
        self._responses.close_put()

        # Keep a reference so the Jobserver (and its slot semaphores) outlives
        # the dispatcher Process, which drops _args after start() in 3.11+.
        self._jobserver = jobserver

        _LOG.debug(
            "Executor started (dispatcher pid=%d)",
            self._dispatcher.pid,
        )

    def __repr__(self) -> str:
        state = "shutdown" if self._shutdown else "active"
        with self._lock:
            pending = len(self._futures)
        return (
            f"JobserverExecutor({state}, pending={pending}"
            f", jobserver={self._jobserver!r})"
        )

    def submit(
        self,
        fn: Callable[..., T],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> concurrent.futures.Future[T]:
        """Submit a callable for execution and return a Future."""
        with self._lock:
            if self._shutdown:
                raise RuntimeError("Cannot submit: executor is shut down")
            future: concurrent.futures.Future[T] = concurrent.futures.Future()
            work_id = next(self._work_ids)
            self._futures[work_id] = future
        # Lock is released before put() to avoid holding it across
        # potentially-slow pickling and IPC.
        try:
            self._requests.put(
                _request.Submit(
                    work_id=work_id, fn=fn, args=args, kwargs=kwargs
                )
            )
        except BrokenPipeError:
            # Dispatcher exited due to a concurrent shutdown(); clean up and
            # raise the same error a caller would see from a post-shutdown
            # submit() that observed the flag in time.
            with self._lock:
                self._futures.pop(work_id, None)
            msg = "Cannot submit: executor is shut down"
            raise RuntimeError(msg) from None
        except Exception:
            with self._lock:
                self._futures.pop(work_id, None)
            raise
        return future

    # TODO: Add @typing.override when Python 3.12+ is the minimum.
    def map(
        self,
        fn: Callable[..., T],
        /,
        *iterables: Any,
        timeout: Optional[float] = None,
        chunksize: int = 1,
        buffersize: Optional[int] = None,
    ) -> Iterator[T]:
        """Return an iterator of fn applied to each iterables entry.

        Calls may be evaluated out-of-order.  Raises TimeoutError if
        results cannot be generated before timeout seconds elapse.
        Chunksize groups calls into batches sent to each worker.
        Buffersize limits outstanding submissions; None collects all
        inputs eagerly.

        Overrides Executor.map() for efficiency.
        """
        return self._jobserver.map(
            fn=fn,
            argses=zip(*iterables),
            timeout=timeout,
            chunksize=chunksize,
            buffersize=buffersize,
        )

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        """Shut down the executor, optionally cancelling pending futures."""
        with self._lock:
            already = self._shutdown
            self._shutdown = True
        if not already:
            _LOG.debug(
                "Shutdown requested (wait=%s, cancel_futures=%s)",
                wait,
                cancel_futures,
            )
            try:
                if cancel_futures:
                    self._requests.put(_request.Cancel())
                self._requests.put(_request.Shutdown())
            except BrokenPipeError:
                _LOG.debug("Dispatcher already exited before shutdown message")

        if wait:
            self._dispatcher.join()
            self._receiver.join()

    # ---- Receiver thread (bridges responses to c.f.Futures) ----

    def _receive_loop(self) -> None:
        """Drain response queue, completing c.f.Futures as results arrive."""
        while True:
            try:
                msg = self._responses.get(timeout=None)
            except EOFError:
                break

            if isinstance(msg, _response.Shutdown):
                break
            elif isinstance(msg, _response.Started):
                with self._lock:
                    future = self._futures.get(msg.work_id)
                if future is not None:
                    # Returns False when the future was cancelled before this
                    # Started message arrived.  The work is already running
                    # in the dispatcher; the eventual Completed or Failed
                    # will be silently discarded by the cancelled() check
                    # below, so no further action is needed here.
                    future.set_running_or_notify_cancel()
            elif isinstance(msg, _response.Completed):
                with self._lock:
                    future = self._futures.pop(msg.work_id, None)
                if future is not None and not future.cancelled():
                    future.set_result(msg.value)
            elif isinstance(msg, _response.Failed):
                with self._lock:
                    future = self._futures.pop(msg.work_id, None)
                if future is not None and not future.cancelled():
                    future.set_exception(msg.exc)
            elif isinstance(msg, _response.Cancelled):
                with self._lock:
                    future = self._futures.pop(msg.work_id, None)
                if future is not None:
                    future.cancel()
                    future.set_running_or_notify_cancel()
            else:
                raise RuntimeError(f"Unexpected response type: {type(msg)!r}")

        # Fail any futures still outstanding because the dispatcher
        # exited without sending results for them (crash or unexpected exit).
        with self._lock:
            remaining = list(self._futures.values())
            self._futures.clear()
        _LOG.debug(
            "Dispatcher exited with %d outstanding futures",
            len(remaining),
        )
        for future in remaining:
            if future.done():
                continue
            try:
                future.set_running_or_notify_cancel()
            except concurrent.futures.InvalidStateError:
                pass
            try:
                future.set_exception(
                    RuntimeError(
                        "Dispatcher process exited before"
                        " delivering this future's result"
                    )
                )
            except concurrent.futures.InvalidStateError:
                pass


# ---- Dispatcher process (runs in a child process) ----


def _dispatch_loop(
    jobserver: Jobserver,
    requests: MinimalQueue,
    responses: MinimalQueue,
) -> None:
    """Main loop for the dispatcher process."""
    _LOG.debug("Dispatcher process started")

    # Child only reads from requests and writes responses
    requests.close_put()
    responses.close_get()

    pending: deque[_request.Submit] = deque()
    running: dict[Future, int] = {}

    shutdown = False
    while not shutdown:
        shutdown = _drain_requests(requests, pending, running, responses)
        _dispatch_pending(jobserver, pending, running, responses)
        _poll_running(running, responses)
        if not shutdown:
            shutdown = _poll_requests_briefly(
                requests, pending, running, responses
            )

    _handle_shutdown(pending, running, responses)
    _LOG.debug("Dispatcher process exiting")


def _handle_request(
    msg: object,
    pending: deque[_request.Submit],
    responses: MinimalQueue,
) -> bool:
    """Handle a single request message.  Return True on Shutdown."""
    if isinstance(msg, _request.Shutdown):
        return True
    if isinstance(msg, _request.Cancel):
        for item in pending:
            responses.put(_response.Cancelled(work_id=item.work_id))
        pending.clear()
    elif isinstance(msg, _request.Submit):
        pending.append(msg)
    else:
        raise RuntimeError(f"Unexpected request type: {type(msg)!r}")
    return False


def _drain_requests(
    requests: MinimalQueue,
    pending: deque[_request.Submit],
    running: dict[Future, int],
    responses: MinimalQueue,
) -> bool:
    """Drain the request queue.  Return True when shutdown requested."""
    while True:
        # Block only when there is nothing else to do
        block = (not pending) and (not running)
        try:
            msg = requests.get(timeout=None if block else 0)
        except queue.Empty:
            return False
        except EOFError:
            return True  # Parent died, treat as shutdown
        if _handle_request(msg, pending, responses):
            return True


def _dispatch_pending(
    jobserver: Jobserver,
    pending: deque[_request.Submit],
    running: dict[Future, int],
    responses: MinimalQueue,
) -> None:
    """Dispatch pending work in place via popleft().

    Keeps c.f.Future in PENDING (cancellable) until a process is
    spawned.  Stops on first Blocked since remaining will be too.
    """
    while pending:
        item = pending[0]
        try:
            f = jobserver.submit(
                fn=item.fn,
                args=item.args,
                kwargs=dict(item.kwargs),
                callbacks=True,
                timeout=0,
            )
        except Blocked:
            return
        except Exception as exc:
            # Dispatch itself failed (e.g. pickling error).
            # Transition PENDING -> RUNNING -> FINISHED(exc).
            pending.popleft()
            responses.put(_response.Started(work_id=item.work_id))
            responses.put(_response.Failed(work_id=item.work_id, exc=exc))
            continue

        # Dispatch succeeded -- inform receiver and track
        pending.popleft()
        responses.put(_response.Started(work_id=item.work_id))
        running[f] = item.work_id


def _poll_running(
    running: dict[Future, int],
    responses: MinimalQueue,
) -> None:
    """Poll running Futures and bridge completed results."""
    completed: list[Future] = []
    for f in running:
        try:
            if f.done():
                completed.append(f)
        except CallbackRaised:
            # Internal callbacks should not raise, but recover
            completed.append(f)

    for f in completed:
        work_id = running.pop(f)
        _bridge_result(f, work_id, responses)


def _bridge_result(
    f: Future,
    work_id: int,
    responses: MinimalQueue,
) -> None:
    """Transfer a completed jobserver Future's outcome to response queue."""
    try:
        value = f.result(timeout=0)
        responses.put(_response.Completed(work_id=work_id, value=value))
    except Exception as exc:
        responses.put(_response.Failed(work_id=work_id, exc=exc))


def _handle_shutdown(
    pending: deque[_request.Submit],
    running: dict[Future, int],
    responses: MinimalQueue,
) -> None:
    """Cancel pending work, drain running futures, signal completion."""
    for item in pending:
        responses.put(_response.Cancelled(work_id=item.work_id))
    for f, work_id in running.items():
        while True:
            try:
                f.wait(timeout=None)
                break
            except CallbackRaised:
                continue
        _bridge_result(f, work_id, responses)
    responses.put(_response.Shutdown())


def _poll_requests_briefly(
    requests: MinimalQueue,
    pending: deque[_request.Submit],
    running: dict[Future, int],
    responses: MinimalQueue,
) -> bool:
    """Brief blocking poll to pick up new work without busy-spinning.

    Returns True when shutdown was requested.
    """
    if not (running or pending):
        return False
    try:
        msg = requests.get(timeout=0.005)
    except (queue.Empty, EOFError):
        return False
    return _handle_request(msg, pending, responses)
