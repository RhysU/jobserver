# Copyright (C) 2019-2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""A concurrent.futures.Executor backed by a Jobserver."""
import concurrent.futures
import logging
import queue
import threading
import typing

from .impl import Blocked, CallbackRaised, Jobserver
from .impl import Future as JobserverFuture

T = typing.TypeVar("T")

_LOGGER = logging.getLogger(__name__)

# Sentinel signaling the dispatcher to shut down
_SHUTDOWN = object()


class JobserverExecutor(concurrent.futures.Executor):
    """A concurrent.futures.Executor that delegates to a Jobserver.

    Decouples submission from dispatch so that returned
    concurrent.futures.Future instances are genuinely PENDING and
    cancellable before execution begins.  A single dispatcher thread
    drains the submission queue, dispatches work via the Jobserver, and
    polls for completions using only public Jobserver/Future API.
    """

    def __init__(self, jobserver: Jobserver) -> None:
        self._jobserver = jobserver
        # Queue of work items: (_SHUTDOWN | _CancelPending | _WorkItem)
        self._queue: queue.SimpleQueue = queue.SimpleQueue()
        self._shutdown_lock = threading.Lock()
        self._shutdown = False

        self._dispatcher = threading.Thread(
            target=self._dispatch_loop,
            daemon=True,
            name="JobserverExecutor-dispatcher",
        )
        self._dispatcher.start()

    def submit(  # type: ignore[override]
        self,
        fn: typing.Callable[..., T],
        /,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> "concurrent.futures.Future[T]":
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("cannot submit after shutdown")
            future: concurrent.futures.Future[T] = concurrent.futures.Future()
            self._queue.put(_WorkItem(future, fn, args, kwargs))
            return future

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        with self._shutdown_lock:
            self._shutdown = True
        if cancel_futures:
            self._queue.put(_CancelPending())
        self._queue.put(_SHUTDOWN)
        if wait:
            self._dispatcher.join()

    def _dispatch_loop(self) -> None:
        # pending: work items dequeued but not yet dispatched (Blocked)
        pending: typing.List[_WorkItem] = []
        # in_flight: mapping from jobserver Future to c.f.Future
        in_flight: typing.Dict[JobserverFuture, concurrent.futures.Future] = {}

        while True:
            # ---- Phase 1: Drain the submission queue ----
            shutdown_requested = False
            while True:
                # Block only when there is nothing else to do
                block = (not pending) and (not in_flight)
                try:
                    item = self._queue.get(block=block, timeout=None)
                except queue.Empty:
                    break
                if item is _SHUTDOWN:
                    shutdown_requested = True
                    break
                if isinstance(item, _CancelPending):
                    for work in pending:
                        work.future.cancel()
                    pending.clear()
                    continue
                pending.append(item)

            # ---- Phase 2: Attempt to dispatch pending work ----
            #
            # Order: try Jobserver.submit(timeout=0) first.  Only on
            # success do we call set_running_or_notify_cancel().  This
            # keeps the c.f.Future in PENDING (and thus cancellable)
            # until we are certain a process has been spawned for it.
            #
            # If set_running_or_notify_cancel() returns False the
            # Future was cancelled after we dispatched the process.
            # The process will run to completion anyway (Jobserver
            # reclaims its slot via internal callbacks) but we simply
            # discard the mapping -- nobody will read the result.
            still_pending: typing.List[_WorkItem] = []
            blocked = False
            for work in pending:
                # Skip work whose Future was cancelled while in the queue
                if work.future.cancelled():
                    continue
                # Once one item blocks, remaining will too
                if blocked:
                    still_pending.append(work)
                    continue
                try:
                    js_future = self._jobserver.submit(
                        fn=work.fn,
                        args=work.args,
                        kwargs=dict(work.kwargs),
                        callbacks=True,
                        timeout=0,
                    )
                except Blocked:
                    still_pending.append(work)
                    blocked = True
                    continue
                except Exception as exc:
                    # Dispatch itself failed (e.g. pickling error).
                    # Transition PENDING -> RUNNING -> FINISHED(exc).
                    if work.future.set_running_or_notify_cancel():
                        work.future.set_exception(exc)
                    continue

                # Dispatch succeeded -- transition the c.f.Future
                if work.future.set_running_or_notify_cancel():
                    in_flight[js_future] = work.future
                # else: cancelled between submit() and here; process
                # runs but result is discarded (slot reclaimed by
                # Jobserver internal callbacks on the js_future)

            pending = still_pending

            # ---- Phase 3: Poll in-flight jobserver Futures ----
            completed: typing.List[JobserverFuture] = []
            for js_future in in_flight:
                try:
                    if js_future.done(timeout=0):
                        completed.append(js_future)
                except CallbackRaised:
                    # Internal callbacks should not raise, but recover
                    completed.append(js_future)

            for js_future in completed:
                cf_future = in_flight.pop(js_future)
                _bridge_result(js_future, cf_future)

            # ---- Phase 4: Check for shutdown ----
            if shutdown_requested:
                for work in pending:
                    work.future.cancel()
                # Wait for all in-flight work to finish
                for js_future, cf_future in in_flight.items():
                    _drain_and_bridge(js_future, cf_future)
                return

            # ---- Phase 5: Brief sleep / queue drain ----
            # When there is in-flight or pending work, block on the
            # submission queue with a short timeout.  This serves as
            # both a poll interval and a way to pick up new work or
            # shutdown signals without busy-spinning.
            if in_flight or pending:
                try:
                    item = self._queue.get(block=True, timeout=0.005)
                    if item is _SHUTDOWN:
                        self._queue.put(_SHUTDOWN)
                    elif isinstance(item, _CancelPending):
                        for work in pending:
                            work.future.cancel()
                        pending = [
                            w for w in pending if not w.future.cancelled()
                        ]
                    else:
                        pending.append(item)
                except queue.Empty:
                    pass


class _WorkItem:
    """Bundles a c.f.Future with the callable and arguments to dispatch."""

    __slots__ = ("future", "fn", "args", "kwargs")

    def __init__(
        self,
        future: concurrent.futures.Future,
        fn: typing.Callable,
        args: tuple,
        kwargs: dict,
    ) -> None:
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs


class _CancelPending:
    """Sentinel instructing the dispatcher to cancel all pending work."""

    pass


def _bridge_result(
    js_future: JobserverFuture,
    cf_future: "concurrent.futures.Future",
) -> None:
    """Transfer a completed jobserver Future's outcome to a c.f.Future."""
    if cf_future.cancelled():
        return
    try:
        result = js_future.result(timeout=0)
        cf_future.set_result(result)
    except Exception as exc:
        cf_future.set_exception(exc)


def _drain_and_bridge(
    js_future: JobserverFuture,
    cf_future: "concurrent.futures.Future",
) -> None:
    """Block until a jobserver Future completes, then bridge its result."""
    while True:
        try:
            js_future.done(timeout=None)
            break
        except CallbackRaised:
            continue
    _bridge_result(js_future, cf_future)
