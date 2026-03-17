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

from jobserver.impl import Blocked, CallbackRaised, Jobserver, MinimalQueue
from jobserver.impl import Future as JobserverFuture

T = typing.TypeVar("T")

_LOGGER = logging.getLogger(__name__)

# ---- Message tags for inter-process communication ----

# Request tags (main process -> dispatcher process)
_SUBMIT = "submit"  # (_SUBMIT, work_id, fn, args, kwargs)
_CANCEL_PENDING = "cancel_pending"  # (_CANCEL_PENDING,)
_REQ_SHUTDOWN = "req_shutdown"  # (_REQ_SHUTDOWN,)

# Response tags (dispatcher process -> main process)
_RUNNING = "running"  # (_RUNNING, work_id)
_RESULT = "result"  # (_RESULT, work_id, value)
_EXCEPTION = "exception"  # (_EXCEPTION, work_id, exc)
_CANCELLED = "cancelled"  # (_CANCELLED, work_id)
_RESP_SHUTDOWN = "resp_shutdown"  # (_RESP_SHUTDOWN,)


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
        self._shutdown_lock = threading.Lock()
        self._shutdown = False
        self._next_work_id = 0
        self._futures: typing.Dict[int, concurrent.futures.Future] = {}
        self._futures_lock = threading.Lock()

        context = jobserver._context
        self._request_queue: MinimalQueue = MinimalQueue(context)
        self._response_queue: MinimalQueue = MinimalQueue(context)

        self._dispatcher = context.Process(
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
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("cannot submit after shutdown")
            future: concurrent.futures.Future[T] = concurrent.futures.Future()
            work_id = self._next_work_id
            self._next_work_id += 1
            with self._futures_lock:
                self._futures[work_id] = future
            try:
                self._request_queue.put(
                    (_SUBMIT, work_id, fn, args, kwargs)
                )
            except Exception:
                with self._futures_lock:
                    self._futures.pop(work_id, None)
                raise
            return future

    def shutdown(
        self, wait: bool = True, *, cancel_futures: bool = False
    ) -> None:
        with self._shutdown_lock:
            already = self._shutdown
            self._shutdown = True
        if not already:
            try:
                if cancel_futures:
                    self._request_queue.put((_CANCEL_PENDING,))
                self._request_queue.put((_REQ_SHUTDOWN,))
            except BrokenPipeError:
                pass  # Dispatcher already exited
        if wait:
            self._dispatcher.join()
            self._receiver.join()

    # ---- Receiver thread (bridges responses to c.f.Futures) ----

    def _receive_loop(self) -> None:
        """Drain the response queue, completing c.f.Futures as results arrive."""
        while True:
            try:
                msg = self._response_queue.get(timeout=None)
            except EOFError:
                break

            tag = msg[0]
            if tag == _RESP_SHUTDOWN:
                break
            elif tag == _RUNNING:
                self._on_running(msg[1])
            elif tag == _RESULT:
                self._on_result(msg[1], msg[2])
            elif tag == _EXCEPTION:
                self._on_exception(msg[1], msg[2])
            elif tag == _CANCELLED:
                self._on_cancelled(msg[1])

        self._fail_remaining()

    def _on_running(self, work_id: int) -> None:
        with self._futures_lock:
            future = self._futures.get(work_id)
        if future is not None:
            future.set_running_or_notify_cancel()

    def _on_result(self, work_id: int, value: typing.Any) -> None:
        with self._futures_lock:
            future = self._futures.pop(work_id, None)
        if future is not None and not future.cancelled():
            future.set_result(value)

    def _on_exception(self, work_id: int, exc: Exception) -> None:
        with self._futures_lock:
            future = self._futures.pop(work_id, None)
        if future is not None and not future.cancelled():
            future.set_exception(exc)

    def _on_cancelled(self, work_id: int) -> None:
        with self._futures_lock:
            future = self._futures.pop(work_id, None)
        if future is not None:
            future.cancel()

    def _fail_remaining(self) -> None:
        """Transition any leftover futures to a failed state."""
        with self._futures_lock:
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

    pending: typing.List[typing.Tuple] = []
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
            _handle_shutdown(pending, in_flight, response_queue)
            return
        if _poll_requests_briefly(
            request_queue, pending, in_flight, response_queue
        ):
            _handle_shutdown(pending, in_flight, response_queue)
            return


def _drain_requests(
    request_queue: MinimalQueue,
    pending: typing.List[typing.Tuple],
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

        tag = msg[0]
        if tag == _REQ_SHUTDOWN:
            return True
        if tag == _CANCEL_PENDING:
            for item in pending:
                response_queue.put((_CANCELLED, item[0]))
            pending.clear()
            continue
        if tag == _SUBMIT:
            pending.append(msg[1:])


def _dispatch_pending(
    jobserver: Jobserver,
    pending: typing.List[typing.Tuple],
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> typing.List[typing.Tuple]:
    """Try to dispatch pending work; return items still pending.

    Keeps c.f.Future in PENDING (cancellable) until a process is
    spawned.  Once one item is Blocked, remaining will be too.
    """
    still_pending: typing.List[typing.Tuple] = []
    blocked = False
    for item in pending:
        work_id, fn, args, kwargs = item
        if blocked:
            still_pending.append(item)
            continue
        try:
            js_future = jobserver.submit(
                fn=fn,
                args=args,
                kwargs=dict(kwargs),
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
            response_queue.put((_RUNNING, work_id))
            response_queue.put((_EXCEPTION, work_id, exc))
            continue

        # Dispatch succeeded -- inform receiver and track
        response_queue.put((_RUNNING, work_id))
        in_flight[js_future] = work_id
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
    """Transfer a completed jobserver Future's outcome to the response queue."""
    try:
        result = js_future.result(timeout=0)
        response_queue.put((_RESULT, work_id, result))
    except Exception as exc:
        response_queue.put((_EXCEPTION, work_id, exc))


def _handle_shutdown(
    pending: typing.List[typing.Tuple],
    in_flight: typing.Dict[JobserverFuture, int],
    response_queue: MinimalQueue,
) -> None:
    """Cancel pending work, drain in-flight futures, signal completion."""
    for item in pending:
        response_queue.put((_CANCELLED, item[0]))
    for js_future, work_id in in_flight.items():
        _drain_and_bridge(js_future, work_id, response_queue)
    response_queue.put((_RESP_SHUTDOWN,))


def _drain_and_bridge(
    js_future: JobserverFuture,
    work_id: int,
    response_queue: MinimalQueue,
) -> None:
    """Block until a jobserver Future completes, then bridge its result."""
    while True:
        try:
            js_future.done(timeout=None)
            break
        except CallbackRaised:
            continue
    _bridge_result(js_future, work_id, response_queue)


def _poll_requests_briefly(
    request_queue: MinimalQueue,
    pending: typing.List[typing.Tuple],
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

    tag = msg[0]
    if tag == _REQ_SHUTDOWN:
        return True
    if tag == _CANCEL_PENDING:
        for item in pending:
            response_queue.put((_CANCELLED, item[0]))
        pending.clear()
    elif tag == _SUBMIT:
        pending.append(msg[1:])
    return False
