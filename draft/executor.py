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

from jobserver.impl import Blocked, CallbackRaised, Jobserver
from jobserver.impl import Future as JobserverFuture

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
        pending: typing.List[_WorkItem] = []
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ] = {}

        while True:
            shutdown = self._drain_queue(pending, in_flight)
            pending = self._dispatch_pending(pending, in_flight)
            self._poll_in_flight(in_flight)
            if shutdown:
                self._handle_shutdown(pending, in_flight)
                return
            self._poll_queue_briefly(pending, in_flight)

    def _drain_queue(
        self,
        pending: typing.List["_WorkItem"],
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ],
    ) -> bool:
        """Drain submission queue.  Return True when shutdown requested."""
        while True:
            block = (not pending) and (not in_flight)
            try:
                item = self._queue.get(block=block, timeout=None)
            except queue.Empty:
                return False
            if item is _SHUTDOWN:
                return True
            if isinstance(item, _CancelPending):
                _cancel_all(pending)
                pending.clear()
                continue
            pending.append(item)

    def _dispatch_pending(
        self,
        pending: typing.List["_WorkItem"],
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ],
    ) -> typing.List["_WorkItem"]:
        """Try to dispatch pending work; return items still pending.

        Keeps c.f.Future in PENDING (cancellable) until a process is
        spawned.  Once one item is Blocked, remaining will be too.
        """
        still_pending: typing.List[_WorkItem] = []
        blocked = False
        for work in pending:
            if work.future.cancelled():
                continue
            if blocked:
                still_pending.append(work)
                continue
            blocked = self._try_dispatch_one(work, in_flight, still_pending)
        return still_pending

    def _try_dispatch_one(
        self,
        work: "_WorkItem",
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ],
        still_pending: typing.List["_WorkItem"],
    ) -> bool:
        """Attempt to dispatch a single work item.  Return True if blocked."""
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
            return True
        except Exception as exc:
            if work.future.set_running_or_notify_cancel():
                work.future.set_exception(exc)
            return False

        if work.future.set_running_or_notify_cancel():
            in_flight[js_future] = work.future
        return False

    @staticmethod
    def _poll_in_flight(
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ],
    ) -> None:
        """Poll in-flight Futures and bridge completed results."""
        completed: typing.List[JobserverFuture] = []
        for js_future in in_flight:
            try:
                if js_future.done(timeout=0):
                    completed.append(js_future)
            except CallbackRaised:
                completed.append(js_future)

        for js_future in completed:
            cf_future = in_flight.pop(js_future)
            _bridge_result(js_future, cf_future)

    @staticmethod
    def _handle_shutdown(
        pending: typing.List["_WorkItem"],
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ],
    ) -> None:
        """Cancel pending work and drain all in-flight futures."""
        _cancel_all(pending)
        for js_future, cf_future in in_flight.items():
            _drain_and_bridge(js_future, cf_future)

    def _poll_queue_briefly(
        self,
        pending: typing.List["_WorkItem"],
        in_flight: typing.Dict[
            JobserverFuture, concurrent.futures.Future
        ],
    ) -> None:
        """Brief blocking poll to pick up new work without busy-spinning."""
        if not (in_flight or pending):
            return
        try:
            item = self._queue.get(block=True, timeout=0.005)
        except queue.Empty:
            return
        if item is _SHUTDOWN:
            self._queue.put(_SHUTDOWN)
        elif isinstance(item, _CancelPending):
            _cancel_all(pending)
            pending[:] = [
                w for w in pending if not w.future.cancelled()
            ]
        else:
            pending.append(item)


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


def _cancel_all(pending: typing.List["_WorkItem"]) -> None:
    """Cancel every work item's c.f.Future."""
    for work in pending:
        work.future.cancel()


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
