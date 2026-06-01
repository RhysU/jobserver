# Copyright (C) 2026 Rhys Ulerich <rhys.ulerich@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""A concurrent.futures.Executor backed by a Jobserver."""

import concurrent.futures
import functools
import itertools
import logging
import pickle
import queue
import threading
import traceback
import weakref
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from multiprocessing.connection import Connection, wait
from typing import Any, Optional, TypeVar, Union

from . import _request, _response
from ._compat import ignore_sigpipe
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
    cancellable before execution begins.
    """

    def __init__(self, jobserver: Optional[Jobserver] = None) -> None:
        """Initialize the executor with an optional Jobserver.

        When jobserver is None, a default-constructed Jobserver is
        created and owned by this executor.
        """
        # One lock guards _shutdown, _work_ids, _futures, and _broken together.
        self._lock = threading.Lock()
        self._shutdown = False
        self._work_ids: Iterator[int] = itertools.count()
        self._futures: dict[int, concurrent.futures.Future] = {}
        # The receiver thread's fatal exception, once it dies; submit() then
        # refuses work.
        self._broken: Optional[BaseException] = None

        # Own the jobserver when none is supplied; caller owns it otherwise.
        self._own_jobserver = jobserver is None
        if jobserver is None:
            jobserver = Jobserver()

        self._requests: MinimalQueue = MinimalQueue(jobserver.context)
        self._responses: MinimalQueue = MinimalQueue(jobserver.context)

        self._dispatcher = jobserver.context.Process(  # type: ignore
            target=_dispatch_loop,
            args=(jobserver, self._requests, self._responses),
            daemon=False,
            name="JobserverExecutor-dispatcher",
        )
        self._dispatcher.start()

        # Close before receiver so EOF propagates if receiver fails to start.
        self._requests.close_get()
        self._responses.close_put()

        self._receiver = threading.Thread(
            target=self._receive_loop,
            daemon=True,
            name="JobserverExecutor-receiver",
        )
        self._receiver.start()

        # Keep a reference so the Jobserver (and its slot semaphores) outlives
        # the dispatcher Process, which drops _args after start() in 3.11+.
        self._jobserver = jobserver

        _LOG.debug(
            "Executor started (dispatcher pid=%d)",
            self._dispatcher.pid,
        )

    def __repr__(self) -> str:
        with self._lock:
            if self._broken is not None:
                state = "broken"
            elif self._shutdown:
                state = "shutdown"
            else:
                state = "active"
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
        # Built outside the lock; no external referrer yet, so the critical
        # section need only cover the gate and work_id registration.
        future: concurrent.futures.Future[T] = concurrent.futures.Future()
        with self._lock:
            if self._broken is not None:
                raise concurrent.futures.BrokenExecutor(
                    "Cannot submit: executor is broken"
                ) from self._broken
            if self._shutdown:
                raise RuntimeError("Cannot submit: executor is shut down")
            work_id = next(self._work_ids)
            self._futures[work_id] = future
        # Let a cancelled PENDING future tell the dispatcher to prune it before
        # dispatch; the weak ref avoids pinning the executor alive.  Registered
        # before the future escapes to the caller, so no .cancel() can race it.
        future.add_done_callback(
            functools.partial(_cancel_observer, weakref.ref(self), work_id)
        )
        success = False
        try:
            self._requests.put(
                _request.Submit(
                    work_id=work_id, fn=fn, args=args, kwargs=kwargs
                )
            )
            success = True
        except BrokenPipeError:
            # Issue RuntimeError to match post-shutdown submit() behavior.
            raise RuntimeError(
                "Cannot submit: executor is shut down"
            ) from None
        finally:
            # Lock released before put() to avoid blocking on IPC.  On any
            # failure, remove the orphaned future so the dispatcher never
            # sees a work_id without a request.
            if not success:
                with self._lock:
                    self._futures.pop(work_id, None)
        return future

    def _notify_cancel(self, work_id: int) -> None:
        """Emit Cancel(work_id) so the dispatcher prunes the pending work."""
        with self._lock:
            if self._shutdown:
                # shutdown(cancel_futures=True) already sent a blanket
                # Cancel(); the dispatcher is winding down, and the receiver
                # re-cancels each pending future, so a per-future Cancel here
                # would be redundant.
                return
        try:
            self._requests.put(_request.Cancel(work_id=work_id))
        except (BrokenPipeError, ValueError, OSError):
            # Dispatcher already exited, or the request pipe was closed by a
            # concurrent shutdown between the check above and this put.
            # Either way, there is nothing left to prune.
            pass

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
        """Return an iterator of fn applied to each iterables entry."""
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
        """Shut down the executor, optionally cancelling pending futures.

        Like ProcessPoolExecutor, shutdown(wait=False) is not fire-and-forget:
        the dispatcher keeps the interpreter alive until running work finishes.
        cancel_futures only prunes pending work; running futures are drained.
        """
        with self._lock:
            already = self._shutdown
            self._shutdown = True
        if not already:
            _LOG.debug(
                "Shutdown requested (wait=%s, cancel_futures=%s)",
                wait,
                cancel_futures,
            )
            # Puts are lock-free; a Submit racing this Cancel() is caught by
            # the dispatcher's latch (see _handle_request).
            try:
                if cancel_futures:
                    self._requests.put(_request.Cancel())
                self._requests.put(_request.Shutdown())
            except BrokenPipeError:
                _LOG.debug("Dispatcher already exited before shutdown message")

        if wait:
            self._dispatcher.join()
            self._receiver.join()
            self._requests.close_put()
            self._responses.close_get()
            if self._own_jobserver:
                # Clear ownership before closing so that a second call to
                # shutdown(wait=True) skips this block -- _own_jobserver=False
                # doubles as the "already closed" sentinel, eliminating the
                # need for a separate flag.
                self._own_jobserver = False
                self._jobserver.__exit__(None, None, None)

    # ---- Receiver thread (bridges responses to c.f.Futures) ----

    def _receive_loop(self) -> None:
        """Drain responses; never let the receiver die silently.

        An unhandled exception here would otherwise kill this daemon thread
        and hang every outstanding future.  Fail them with the cause attached,
        then re-raise so the thread still dies loudly via threading.excepthook.
        """
        try:
            self._receive_messages()
        except BaseException as exc:
            self._fail_all(cause=exc)
            raise
        else:
            # Clean exit (Shutdown or EOF): fail any work the dispatcher left.
            self._fail_all(cause=None)

    def _receive_messages(self) -> None:
        """Drain response queue, completing c.f.Futures as results arrive."""
        while True:
            try:
                msg = self._responses.get(timeout=None)
            except EOFError:
                return

            if isinstance(msg, _response.Shutdown):
                return
            elif isinstance(msg, _response.Started):
                with self._lock:
                    future = self._futures.get(msg.work_id)
                if future is not None:
                    # Returns False when the future was cancelled before this
                    # Started message arrived.  The work is already running
                    # in the dispatcher; the eventual Completed or Failed
                    # will be silently discarded by the cancelled() check
                    # below, so no further action is needed here.
                    try:
                        future.set_running_or_notify_cancel()
                    except concurrent.futures.InvalidStateError:
                        pass
            elif isinstance(msg, _response.Completed):
                with self._lock:
                    future = self._futures.pop(msg.work_id, None)
                if future is not None and not future.cancelled():
                    # Type erased to Any; see Completed docstring.
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

    def _fail_all(self, cause: Optional[BaseException]) -> None:
        """Fail every outstanding future; no more results will arrive.

        cause is None when the dispatcher exited; otherwise the receiver died
        and cause is recorded as _broken and chained onto each failed future.
        """
        with self._lock:
            if cause is not None:
                self._broken = cause
            remaining = list(self._futures.values())
            self._futures.clear()
        _LOG.debug(
            "Failing %d outstanding futures (broken=%s)",
            len(remaining),
            cause is not None,
        )
        message = (
            "Receiver thread died before delivering this future's result"
            if cause is not None
            else "Dispatcher process exited before"
            " delivering this future's result"
        )
        err = concurrent.futures.BrokenExecutor(message)
        if cause is not None:
            err.__cause__ = cause
        for future in remaining:
            if future.done():
                continue
            try:
                # set_exception accepts a PENDING or RUNNING future directly.
                future.set_exception(err)
            except concurrent.futures.InvalidStateError:
                pass  # cancelled concurrently after the done() check


def _cancel_observer(
    executor_ref: "weakref.ref[JobserverExecutor]",
    work_id: int,
    future: concurrent.futures.Future,
) -> None:
    """Done-callback that prunes a cancelled PENDING future's work.

    Registered on every submitted future.  Done-callbacks fire exactly once,
    on the state transition, so a future cancelled while PENDING triggers a
    single Cancel(work_id) and the receiver's later re-cancel does not
    re-emit.  Non-cancel completions (result, exception) are ignored, and a
    dead weak reference means the executor is already gone -- nothing to do.
    """
    if not future.cancelled():
        return
    executor = executor_ref()
    if executor is not None:
        executor._notify_cancel(work_id)


# ---- Dispatcher process (runs in a child process) ----


@dataclass
class _DispatchState:
    """Mutable state owned by the dispatcher loop and its handlers.

    Passed by reference and updated in place, so handlers need no return
    values to thread `shutdown` and `cancelling` back to the loop.
    """

    pending: deque[_request.Submit] = field(default_factory=deque)
    running: dict[Future, int] = field(default_factory=dict)
    # Latched by a blanket Cancel(); then racing Submits are cancelled.
    cancelling: bool = False
    # Set by a Shutdown() message or parent EOF; ends the loop.
    shutdown: bool = False


def _dispatch_loop(
    jobserver: Jobserver,
    requests: MinimalQueue,
    responses: MinimalQueue,
) -> None:
    """Main loop for the dispatcher process."""
    _LOG.debug("Dispatcher process started")
    ignore_sigpipe()

    # Child only reads from requests and writes responses
    requests.close_put()
    responses.close_get()

    state = _DispatchState()
    while not state.shutdown:
        _drain_requests(requests, state, responses)
        _dispatch_pending(jobserver, state, responses)
        _poll_running(state, responses)
        if not state.shutdown:
            _poll_requests_briefly(requests, state, responses)

    _handle_shutdown(state, responses)
    _LOG.debug("Dispatcher process exiting")


def _handle_request(
    msg: object,
    state: _DispatchState,
    responses: MinimalQueue,
) -> None:
    """Handle a single request message, updating state in place."""
    if isinstance(msg, _request.Shutdown):
        state.shutdown = True
    elif isinstance(msg, _request.Cancel):
        keep: list[_request.Submit] = []
        for item in state.pending:
            if msg.work_id is None or item.work_id == msg.work_id:
                responses.put(_response.Cancelled(work_id=item.work_id))
            else:
                keep.append(item)
        state.pending.clear()
        state.pending.extend(keep)
        # A blanket Cancel() precedes Shutdown(); latch to cancel late Submits.
        state.cancelling = state.cancelling or msg.work_id is None
    elif isinstance(msg, _request.Submit):
        if state.cancelling:
            responses.put(_response.Cancelled(work_id=msg.work_id))
        else:
            state.pending.append(msg)
    else:
        raise RuntimeError(f"Unexpected request type: {type(msg)!r}")


def _drain_requests(
    requests: MinimalQueue,
    state: _DispatchState,
    responses: MinimalQueue,
) -> None:
    """Drain the request queue, setting state.shutdown on Shutdown or EOF."""
    while True:
        # Block only when there is nothing else to do
        block = (not state.pending) and (not state.running)
        try:
            msg = requests.get(timeout=None if block else 0)
        except queue.Empty:
            return
        except EOFError:
            state.shutdown = True  # Parent died, treat as shutdown
            return
        _handle_request(msg, state, responses)
        if state.shutdown:
            return


def _dispatch_pending(
    jobserver: Jobserver,
    state: _DispatchState,
    responses: MinimalQueue,
) -> None:
    """Dispatch pending work in place via popleft().

    Keeps c.f.Future in PENDING (cancellable) until a process is
    spawned.  Stops on first Blocked since remaining will be too.
    """
    pending = state.pending
    while pending:
        item = pending[0]
        try:
            f = jobserver.submit(
                fn=item.fn,
                args=item.args,
                kwargs=item.kwargs,
                timeout=0,
            )
        except Blocked:
            return
        except Exception as exc:
            # Dispatch itself failed (e.g. pickling error).
            # Transition PENDING -> RUNNING -> FINISHED(exc).
            pending.popleft()
            responses.put(_response.Started(work_id=item.work_id))
            _responses_put_failed(responses, item.work_id, exc)
            continue

        # Dispatch succeeded -- inform receiver and track
        pending.popleft()
        responses.put(_response.Started(work_id=item.work_id))
        state.running[f] = item.work_id


def _poll_running(
    state: _DispatchState,
    responses: MinimalQueue,
) -> None:
    """Poll running Futures and bridge completed results."""
    running = state.running
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


def _responses_put_failed(
    responses: MinimalQueue,
    work_id: int,
    exc: Exception,
) -> None:
    """Put a Failed response, falling back if exc is not picklable."""
    failed = _response.Failed(work_id=work_id, exc=exc)
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    try:
        responses.put(failed)
    # A locally-defined exception class raises AttributeError and an
    # unpicklable payload TypeError, so both join PicklingError here; aligns
    # with _jobserver.py's _worker_entrypoint pickle-failure classification.
    except (pickle.PicklingError, AttributeError, TypeError):
        responses.put(
            _response.Failed(
                work_id=work_id,
                exc=RuntimeError(
                    f"(original exception was not picklable)\n{tb}"
                ),
            )
        )


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
        _responses_put_failed(responses, work_id, exc)


def _handle_shutdown(
    state: _DispatchState,
    responses: MinimalQueue,
) -> None:
    """Cancel pending work, drain running futures, signal completion."""
    for item in state.pending:
        responses.put(_response.Cancelled(work_id=item.work_id))
    for f, work_id in state.running.items():
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
    state: _DispatchState,
    responses: MinimalQueue,
) -> None:
    """Brief blocking poll to pick up new work without busy-spinning.

    Sets state.shutdown when a Shutdown()/EOF is observed.
    """
    if not state.running and not state.pending:
        return

    waitable = requests.waitable()
    augmented: list[Union[Connection, int]] = [waitable]
    augmented.extend(
        f._process.sentinel for f in state.running if f._process is not None
    )

    # 1s timeout is a robustness fallback; normally a sentinel fires first
    if waitable in wait(augmented, timeout=1.0):
        try:
            msg = requests.get(timeout=0)
        except (queue.Empty, EOFError):
            return
        _handle_request(msg, state, responses)
