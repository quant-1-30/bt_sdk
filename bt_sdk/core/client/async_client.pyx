# cython: language_level=3, boundscheck=False, wraparound=False


import asyncio
import logging
from collections import namedtuple

import grpc
import reactivex
from reactivex.subject import Subject

logger = logging.getLogger(__name__)

from bt_sdk.core.rpc.client cimport RpcClient

_PendingCounts = namedtuple("_PendingCounts", "tasks futures")

# bounded wait for in-flight stream tasks to settle during shutdown:
# must stay well below AsyncClient.close()'s outer fut.result(timeout) so
# close() never times out just because the settle-wait ran its full course
_SHUTDOWN_SETTLE_TIMEOUT = 5.0


cdef class AsyncClient:

    def __init__(self):
        
        self._running = True
        self.timeout = 5  # default; subclasses overwrite (base close() reads it)
        self.listen_task = None
        self._shutdown_task = None
        
    cdef void _finalize_task(self, object future):
        try:
            future.result()  # result or exception
        except Exception as e:
            logger.warning(f"[{self.__class__.__name__}] background task error: {e}")

    cdef object wrap_protocol(self, bytes req_id, object msg):
        """implement on protocol"""
        pass

    cpdef object run(self, bytes req_id, object msg):
        if not self._running:
            raise RuntimeError("client is not running")
        
        obs_or_fut = self.wrap_protocol(req_id, msg)
        return obs_or_fut

    async def _async_shutdown(self):
        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass

    cdef _on_cleanup_done(self, task):
            try:
                if not task.cancelled() and task.exception():
                    logger.warning(f"[{self.__class__.__name__}] cleanup error: {task.exception()}")
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] error reading task result: {e}")

    cpdef void close(self):
        if not self._running:
            return
        self._running = False

        cdef object running
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        # fallback running loop
        cdef object loop = self.loop if self.loop is not None else running

        if loop is not None and not loop.is_closed() and loop.is_running():
            try:
                if running is loop:
                    task = loop.create_task(self._async_shutdown())
                    self._shutdown_task = task
                    task.add_done_callback(self._finalize_task)
                    logger.info(f"[{self.__class__.__name__}] Shutdown scheduled from loop thread.")
                else:
                    fut = asyncio.run_coroutine_threadsafe(self._async_shutdown(), loop)
                    fut.result(timeout=self.timeout)
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] shutdown error: {e}")
        else:
            logger.info(f"[{self.__class__.__name__}] No running loop, skipping async shutdown.")

        logger.info(f"[{self.__class__.__name__}] Shutdown complete.")


cdef class AsyncRpcClient(AsyncClient):
    """
    High-performance asynchronous Rpc client
    """
    def __init__(self, tuple addr, int timeout=5):
        super().__init__()
        self.rpc_client = RpcClient(host=addr[0], port=addr[1])
        self.timeout = timeout
        self._connected = False
        self._conn_lock = None  # asyncio.Lock, lazy init in coroutine
        self._pending_tasks = set()  # strong refs to same-loop Tasks: prevent GC + deterministic shutdown cancel
        self._pending_futs = set()   # strong refs to cross-loop concurrent Futures (run_coroutine_threadsafe path)
        self.loop = None

    cpdef void attach_loop(self, loop):
        self.loop = loop
        logger.info(f"[{self.__class__.__name__}] Attached Loop: {id(loop)}")

    cpdef void revive(self):
        """Re-enable a client previously closed via close()/disconnect()."""
        if not self._running:
            self._running = True

    def pending_counts(self):
        """PendingCounts(tasks, futures): #in-flight asyncio Tasks on the
        attached loop / #cross-loop concurrent Futures.

        Read-only observability helper (cdef attributes are invisible to
        Python): used by tests and by ops to detect stream/task leaks.
        Cross-loop streams register in BOTH counters (future from the
        factory, wrapped task once it starts).
        """
        return _PendingCounts(len(self._pending_tasks), len(self._pending_futs))

    cpdef void reset_connection(self):
        """Reset connection state for event loop changes.
        dispose gRPC channel and _ensure_connection() on new loop
        reset_connection run_coroutine_threadsafe from sync MdApi.start
        """
        self._connected = False
        self._conn_lock = None
        cdef object loop = self.loop

        if loop is not None and not loop.is_closed() and loop.is_running():
            try:
                try:
                    running = asyncio.get_running_loop()
                except RuntimeError:
                    running = None
                
                if running is loop:
                    task = loop.create_task(self.rpc_client.cleanup())
                    # not supported lambda
                    task.add_done_callback(self._on_cleanup_done)
                else:
                    fut = asyncio.run_coroutine_threadsafe(self.rpc_client.cleanup(), loop)
                    fut.result(timeout=self.timeout)
                    
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] reset_connection cleanup error: {e}")
        else:
            self.rpc_client.hard_reset()

    async def _async_shutdown(self):
        """listen_task and gRPC channel"""
        cdef list pending

        await super()._async_shutdown()

        # Cancel in-flight stream work deterministically instead of letting it
        # linger until the channel error eventually propagates. Cross-loop
        # concurrent futures first: cancelling them schedules cancellation of
        # the wrapped tasks running on this loop; the settle-wait below then
        # awaits those tasks directly (they self-register in _pending_tasks).
        # NOTE: a factory running on a foreign thread can still add a future
        # after the clear() below (window between close()'s _running=False and
        # this point); its task dies anyway once cleanup() closes the channel,
        # so the leak is bounded to one request's lifetime.
        for fut in list(self._pending_futs):
            if not fut.done():
                fut.cancel()
        self._pending_futs.clear()

        pending = list(self._pending_tasks)
        self._pending_tasks.clear()
        for task in pending:
            if not task.done():
                task.cancel()
        if pending:
            # bounded wait: a wedged task must not block shutdown forever, and
            # must stay strictly below close()'s outer fut.result(timeout=self.timeout).
            settle_timeout = min(_SHUTDOWN_SETTLE_TIMEOUT, max(0.5, self.timeout * 0.5))
            _, still_pending = await asyncio.wait(pending, timeout=settle_timeout)
            if still_pending:
                logger.warning(
                    f"[{self.__class__.__name__}] {len(still_pending)} stream "
                    f"task(s) did not settle within {settle_timeout:.1f}s "
                    "during shutdown; continuing with channel close"
                )

        try:
            await self.rpc_client.cleanup()
        except Exception as e:
            logger.warning(f"[{self.__class__.__name__}] gRPC cleanup error: {e}")
        self._connected = False

    async def _ensure_connection(self):
        """
        Lazy initialize gRPC channel with proper concurrency guard.
        Uses asyncio.Lock to ensure only one coroutine initializes the channel
        even under high concurrency (e.g. asyncio.gather of N factor requests).
        """
        # If an in-loop close() scheduled an async shutdown task right before
        # start() revived the client, wait for it to finish closing the old
        # channel before opening a new one (prevents N2 disconnect->start race).
        if self._shutdown_task is not None and not self._shutdown_task.done():
            try:
                await self._shutdown_task
            except Exception:
                pass
            self._shutdown_task = None

        if self._connected:
            return

        # Lazy init the lock in coroutine context (correct loop binding)
        if self._conn_lock is None:
            self._conn_lock = asyncio.Lock()

        async with self._conn_lock:
            # Double-check after acquiring lock
            if self._connected:
                return
            try:
                logger.info(f"[gRPC] Initializing Channel on Loop: {id(self.loop)}")
                await self.rpc_client.initialize()
                self._connected = True
            except Exception as e:
                logger.error(f"[gRPC Init Error] {e}")
                raise
    
    cdef inline void _safe_on_completed(self, object subject):
        if subject is not None and not subject.is_disposed:
            subject.on_completed()

    cdef inline void _safe_on_error(self, object subject, object error):
        if subject is not None and not subject.is_disposed:
            subject.on_error(error)

    cdef inline object _submit_cross_loop(self, object coro):
        cdef object fut = asyncio.run_coroutine_threadsafe(coro, self.loop)
        self._pending_futs.add(fut)
        return fut

    cdef inline void _cancel_cross_loop(self, object fut):
        if fut is not None:
            self._pending_futs.discard(fut)
            if not fut.done():
                fut.cancel()

    async def _stream_request(self, bytes req_id, object msg, object subject):
        await self._ensure_connection()
        try:
            response_iterator = self.rpc_client.on_request(msg.topic, msg.body)

            async for payload in response_iterator:
                if payload is None:
                    continue
                if subject.is_disposed:
                    break
                subject.on_next({"id": req_id, "data": payload})

            self._safe_on_completed(subject)

        except grpc.aio.AioRpcError as e:
            logger.error(f"[gRPC Error] Code: {e.code()}, Details: {e.details()}")
            self._safe_on_error(subject, e)
        except asyncio.CancelledError:
            # Consumer cancelled / timed out: the Subject is (or is about to
            # be) disposed. Emitting on_completed here would mark a partial
            # buffer as a successful completion — stay silent and propagate.
            logger.info("[gRPC] Request Cancelled")
            raise
        except Exception as e:
            logger.exception(f"[gRPC Unknown Error] {e}")
            self._safe_on_error(subject, e)

    cdef object wrap_protocol(self, bytes req_id, object msg):
        cdef object req_subject = Subject()
        cdef list first_subscription = [True]

        if not self._running:
            raise RuntimeError("client is not running")

        def factory(observer, scheduler):
            # Use the returned req_subject for the first subscription (so
            # callers holding `(observable, req_subject)` can still inspect or
            # dispose it), and allocate an isolated Subject for any subsequent
            # or concurrent subscription so resubscriptions / multi-subscribers
            # never cross-talk or hit an already-completed/disposed Subject.
            if first_subscription[0] and not req_subject.is_disposed:
                first_subscription[0] = False
                sub_subject = req_subject
            else:
                sub_subject = Subject()

            async def run():
                # Self-register the actual asyncio Task so _async_shutdown's
                # bounded settle-wait covers BOTH paths (same-loop tasks and
                # cross-loop futures): the concurrent Future cancel above only
                # *schedules* task.cancel(); waiting on the tasks themselves
                # confirms the stream ended BEFORE the channel is closed.
                task = asyncio.current_task()
                if task is not None:
                    self._pending_tasks.add(task)
                try:
                    await self._stream_request(req_id, msg, sub_subject)
                except asyncio.CancelledError:
                    # already handled inside _stream_request; do not double-report
                    raise
                except Exception as e:
                    if not sub_subject.is_disposed:
                        sub_subject.on_error(e)
                finally:
                    # release strong refs to prevent Task/Future accumulation
                    if task is not None:
                        self._pending_tasks.discard(task)
                    if task_ref[0] is not None:
                        self._pending_tasks.discard(task_ref[0])
                    if fut_ref[0] is not None:
                        self._pending_futs.discard(fut_ref[0])

            # avoid data loss
            sub_subscription = sub_subject.subscribe(observer)

            # ==============================================================
            # Dynamic Thread Detection
            # ==============================================================
            if not self._running or self.loop is None:
                err_msg = (
                    "client is not running"
                    if not self._running
                    else "no event loop attached to AsyncRpcClient"
                )
                sub_subject.on_error(RuntimeError(err_msg))
                sub_subscription.dispose()
                if not sub_subject.is_disposed:
                    sub_subject.dispose()
                return

            task_ref = [None]
            fut_ref = [None]

            def teardown():
                # _pending_tasks removal is left to run()'s finally so an
                # overlapping _async_shutdown still awaits unwinding tasks.
                try:
                    task = task_ref[0]
                    if task is not None:
                        self.loop.call_soon_threadsafe(task.cancel)
                    elif fut_ref[0] is not None:
                        self._cancel_cross_loop(fut_ref[0])
                except RuntimeError:
                    pass
                finally:
                    sub_subscription.dispose()
                    if not sub_subject.is_disposed:
                        sub_subject.dispose()

            try:
                curr_loop = asyncio.get_running_loop()
            except RuntimeError:
                curr_loop = None

            if curr_loop is self.loop:
                # Case 1: same loop (e.g. pytest)
                task = self.loop.create_task(run())
                task_ref[0] = task
                self._pending_tasks.add(task)
            else:
                # Case 2 & 3: different loop or sync context (e.g. Cerebro)
                fut_ref[0] = self._submit_cross_loop(run())

            return teardown

        observable = reactivex.create(factory)
        return (observable, req_subject)

    async def _iter_batches(self, object msg):
        """Shared core generator: registers the running Task in _pending_tasks,
        ensures the gRPC channel is connected, and yields non-empty Arrow tables."""
        if not self._running:
            raise RuntimeError("client is not running")
        cdef object task = asyncio.current_task()
        if task is not None:
            self._pending_tasks.add(task)
        try:
            await self._ensure_connection()
            response_iterator = self.rpc_client.on_request(msg.topic, msg.body)
            async for payload in response_iterator:
                if payload is not None:
                    yield payload
        finally:
            if task is not None:
                self._pending_tasks.discard(task)

    async def direct_run_async(self, bytes req_id, object msg):
        cdef list buffer = []
        try:
            async for payload in self._iter_batches(msg):
                buffer.append(payload)
            return buffer
        except grpc.aio.AioRpcError as e:
            logger.error(f"[gRPC Direct Error] Code: {e.code()}, Details: {e.details()}")
            raise e
        except Exception as e:
            logger.exception(f"[gRPC Direct Unknown Error] {e}")
            raise e

    async def fetch_tables_async(self, bytes req_id, object msg, int timeout):
        """Direct coroutine query across same-loop or cross-loop without RxPY
        Subject/Observable allocation, with full task/future tracking."""
        if not self._running:
            raise RuntimeError("client is not running")

        try:
            curr_loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("[MdApi] rpc_async must be awaited inside a running event loop!") from None

        async def _internal_run():
            return await self.direct_run_async(req_id, msg)

        if curr_loop is self.loop:
            return await asyncio.wait_for(_internal_run(), timeout=timeout)

        fut = self._submit_cross_loop(asyncio.wait_for(_internal_run(), timeout=timeout))
        try:
            return await asyncio.wrap_future(fut)
        finally:
            self._cancel_cross_loop(fut)

    async def stream_batches_async(self, object msg, int max_queue=64):
        """Backpressure-aware async generator yielding Arrow Table batches on
        the caller's own event loop (never blocks GlobalAsyncLoop)."""
        if not self._running:
            raise RuntimeError("client is not running")
        if self.loop is None or self.loop.is_closed():
            raise RuntimeError("no usable event loop attached to AsyncRpcClient")

        curr_loop = asyncio.get_running_loop()
        cdef int bound = max_queue if max_queue > 0 else 64

        if curr_loop is self.loop:
            async for payload in self._iter_batches(msg):
                yield payload
            return

        # Cross-loop path: `q` is touched ONLY on curr_loop; `slots` Semaphore
        # is touched ONLY on self.loop, bounding in-flight batches to `bound`
        # so a slow consumer pauses the gRPC reader coroutine (HTTP/2 backpressure).
        q = asyncio.Queue()
        slots_holder = [None]

        async def _produce():
            slots = asyncio.Semaphore(bound)
            slots_holder[0] = slots
            try:
                async for payload in self._iter_batches(msg):
                    await slots.acquire()
                    curr_loop.call_soon_threadsafe(q.put_nowait, (0, payload))
                curr_loop.call_soon_threadsafe(q.put_nowait, (1, None))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                curr_loop.call_soon_threadsafe(q.put_nowait, (2, e))

        fut = self._submit_cross_loop(_produce())
        try:
            while True:
                kind, item = await q.get()
                if kind == 0:
                    if not self.loop.is_closed():
                        try:
                            self.loop.call_soon_threadsafe(slots_holder[0].release)
                        except RuntimeError:
                            pass
                    yield item
                elif kind == 1:
                    break
                else:
                    raise item
        finally:
            self._cancel_cross_loop(fut)
