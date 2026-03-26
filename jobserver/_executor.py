# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""A concurrent.futures.Executor backed by a Jobserver."""
import concurrent.futures
import itertools
import queue
import threading
import typing

from ._jobserver import (
    Blocked,
    CallbackRaised,
    Jobserver,
    MinimalQueue,
)
from ._jobserver import Future as JobserverFuture

from . import _request
from . import _response

__all__ = ("JobserverExecutor",)

T = typing.TypeVar("T")


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
        # One lock guards _shutdown, _work_ids, and _futures together.
        self._lock = threading.Lock()
        self._shutdown = False
        self._work_ids: typing.Iterator[int] = itertools.count()
        self._futures: typing.Dict[int, concurrent.futures.Future] = {}

        context = jobserver._context
        self._request_queue: MinimalQueue = MinimalQueue(context)
        self._response_queue: MinimalQueue = MinimalQueue(context)

        self._dispatcher = context.Process(  # type: ignore
            target=_dispatch_loop,
            args=(jobserver, self._request_queue, self._response_queue),
            daemon=False,
            name="JobserverExecutor-dispatcher",
        )
        self._dispatcher.start()

        # Close unused pipe ends so EOF propagates on crash.
        # Parent only writes to request_queue and reads response_queue.
        self._request_queue._reader.close()
        self._response_queue._writer.close()

        self._receiver = threading.Thread(
            target=self._receive_loop,
            daemon=True,
            name="JobserverExecutor-receiver",
        )
        self._receiver.start()

    def submit(  # type: ignore[override]
        self,
        fn: typing.Callable[..., T],
        /,
        *args: typing.Any,
        **kwargs: typing.Any,
    ) -> "concurrent.futures.Future[T]":
        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot submit after shutdown")
            future: concurrent.futures.Future[T] = concurrent.futures.Future()
            work_id = next(self._work_ids)
            self._futures[work_id] = future
        # Lock is released before put() to avoid holding it across
        # potentially-slow pickling and IPC.
        try:
            self._request_queue.put(
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
            raise RuntimeError("cannot submit after shutdown")
        except Exception:
            with self._lock:
                self._futures.pop(work_id, None)
            raise
        return future

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        with self._lock:
            already = self._shutdown
            self._shutdown = True
        if not already:
            try:
                if cancel_futures:
                    self._request_queue.put(_request.Cancel())
                self._request_queue.put(_request.Shutdown())
            except BrokenPipeError:
                pass  # Dispatcher already exited
        if wait:
            self._dispatcher.join()
            self._receiver.join()

    # ---- Receiver thread (bridges responses to c.f.Futures) ----

    def _receive_loop(self) -> None:
        """Drain response queue, completing c.f.Futures as results arrive."""
        while True:
            try:
                msg = self._response_queue.get(timeout=None)
            except EOFError:
                break

            if isinstance(msg, _response.Shutdown):
                break
            elif isinstance(msg, _response.Started):
                with self._lock:
                    future = self._futures.get(msg.work_id)
                if future is not None:
                    # Returns False when the future was cancelled before this
                    # Started message arrived.  The work is already in flight
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

        # Fail any futures still outstanding (dispatcher crash)
        with self._lock:
            remaining = list(self._futures.values())
            self._futures.clear()
        for future in remaining:
            if future.done():
                continue
            try:
                future.set_running_or_notify_cancel()
            except concurrent.futures.InvalidStateError:
                pass
            try:
                future.set_exception(
                    RuntimeError("dispatcher process terminated")
                )
            except concurrent.futures.InvalidStateError:
                pass


# ---- Dispatcher process (runs in a child process) ----


def _dispatch_loop(
    jobserver: Jobserver,
    request_queue: MinimalQueue,
    response_queue: MinimalQueue,
) -> None:
    """Main loop for the dispatcher process."""
    # Close unused pipe ends so EOF propagates on crash.
    # Child only reads from request_queue and writes response_queue.
    request_queue._writer.close()
    response_queue._reader.close()

    pending: typing.List[_request.Submit] = []
    in_flight: typing.Dict[JobserverFuture, int] = {}

    while True:
        shutdown = _drain_requests(
            request_queue, pending, in_flight, response_queue
        )
        pending = _dispatch_pending(
            jobserver, pending, in_flight, response_queue
        )
        _poll_in_flight(in_flight, response_queue)
        if shutdown:
            break
        if _poll_requests_briefly(
            request_queue, pending, in_flight, response_queue
        ):
            break

    _handle_shutdown(pending, in_flight, response_queue)


def _drain_requests(
    request_queue: MinimalQueue,
    pending: typing.List[_request.Submit],
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> bool:
    """Drain the request queue.  Return True when shutdown requested."""
    while True:
        # Block only when there is nothing else to do
        block = (not pending) and (not in_flight)
        try:
            msg = request_queue.get(timeout=None if block else 0)
        except queue.Empty:
            return False
        except EOFError:
            return True  # Parent died, treat as shutdown

        if isinstance(msg, _request.Shutdown):
            return True
        if isinstance(msg, _request.Cancel):
            for item in pending:
                response_queue.put(_response.Cancelled(work_id=item.work_id))
            pending.clear()
            continue
        if isinstance(msg, _request.Submit):
            pending.append(msg)


def _dispatch_pending(
    jobserver: Jobserver,
    pending: typing.List[_request.Submit],
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> typing.List[_request.Submit]:
    """Try to dispatch pending work; return items still pending.

    Keeps c.f.Future in PENDING (cancellable) until a process is
    spawned.  Once one item is Blocked, remaining will be too.
    """
    still_pending: typing.List[_request.Submit] = []
    blocked = False
    for item in pending:
        if blocked:
            still_pending.append(item)
            continue
        try:
            js_future = jobserver.submit(
                fn=item.fn,
                args=item.args,
                kwargs=dict(item.kwargs),
                callbacks=True,
                timeout=0,
            )
        except Blocked:
            still_pending.append(item)
            blocked = True
            continue
        except Exception as exc:
            # Dispatch itself failed (e.g. pickling error).
            # Transition PENDING -> RUNNING -> FINISHED(exc).
            response_queue.put(_response.Started(work_id=item.work_id))
            response_queue.put(_response.Failed(work_id=item.work_id, exc=exc))
            continue

        # Dispatch succeeded -- inform receiver and track
        response_queue.put(_response.Started(work_id=item.work_id))
        in_flight[js_future] = item.work_id
    return still_pending


def _poll_in_flight(
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> None:
    """Poll in-flight Futures and bridge completed results."""
    completed: typing.List[JobserverFuture] = []
    for js_future in in_flight:
        try:
            if js_future.done(timeout=0):
                completed.append(js_future)
        except CallbackRaised:
            # Internal callbacks should not raise, but recover
            completed.append(js_future)

    for js_future in completed:
        work_id = in_flight.pop(js_future)
        _bridge_result(js_future, work_id, response_queue)


def _bridge_result(
    js_future: JobserverFuture,
    work_id: int,
    response_queue: MinimalQueue,
) -> None:
    """Transfer a completed jobserver Future's outcome to response queue."""
    try:
        value = js_future.result(timeout=0)
        response_queue.put(_response.Completed(work_id=work_id, value=value))
    except Exception as exc:
        response_queue.put(_response.Failed(work_id=work_id, exc=exc))


def _handle_shutdown(
    pending: typing.List[_request.Submit],
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> None:
    """Cancel pending work, drain in-flight futures, signal completion."""
    for item in pending:
        response_queue.put(_response.Cancelled(work_id=item.work_id))
    for js_future, work_id in in_flight.items():
        while True:
            try:
                js_future.done(timeout=None)
                break
            except CallbackRaised:
                continue
        _bridge_result(js_future, work_id, response_queue)
    response_queue.put(_response.Shutdown())


def _poll_requests_briefly(
    request_queue: MinimalQueue,
    pending: typing.List[_request.Submit],
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> bool:
    """Brief blocking poll to pick up new work without busy-spinning.

    Returns True when shutdown was requested.
    """
    if not (in_flight or pending):
        return False
    try:
        msg = request_queue.get(timeout=0.005)
    except (queue.Empty, EOFError):
        return False

    if isinstance(msg, _request.Shutdown):
        return True
    if isinstance(msg, _request.Cancel):
        for item in pending:
            response_queue.put(_response.Cancelled(work_id=item.work_id))
        pending.clear()
    elif isinstance(msg, _request.Submit):
        pending.append(msg)
    return False
