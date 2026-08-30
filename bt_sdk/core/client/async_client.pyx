# cython: language_level=3, boundscheck=False, wraparound=False


import os
import socket
import asyncio
import reactivex
import uuid
import zmq
import zmq.asyncio
import threading
import logging
import pyarrow as pa
import pyarrow.compute as pc
import reactivex.operators as ops
import grpc
from reactivex import of
from reactivex.subject import Subject
from reactivex.scheduler.eventloop import AsyncIOScheduler
from concurrent.futures import Future, ThreadPoolExecutor

logger = logging.getLogger(__name__)

from bt_protocol._protocol import _ENCODER, _RespDECODER
from bt_sdk.core.rpc.client cimport RpcClient


cdef class AsyncClient:

    def __init__(self):
        
        self._running = True
        self.listen_task = None
        
    cdef void _finalize_task(self, object future):
        try:
            ret = future.result()  # result or exception
        except Exception as e:
            print(f"_finalize_task: {e}")

    cdef object wrap_protocol(self, bytes req_id, object msg):
        """implement on protocol"""
        pass    

    async def send_request(self, bytes req_id, dict message):
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
        self._pending_tasks = set()  # hold strong refs to Tasks to prevent GC
        self.loop = None
        
    cpdef void attach_loop(self, loop):
        self.loop = loop
        print(f"[{self.__class__.__name__}] Attached Loop: {id(loop)}")

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
                running = asyncio._get_running_loop()
                
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
        await super()._async_shutdown()
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
                print(f"[gRPC] Initializing Channel on Loop: {id(self.loop)}")
                await self.rpc_client.initialize()
                self._connected = True
            except Exception as e:
                print(f"[gRPC Init Error] {e}")
                raise e
    
    cdef inline void _safe_on_completed(self, object subject):
        if subject is not None and not subject.is_disposed:
            subject.on_completed()

    cdef inline void _safe_on_error(self, object subject, object error):
        if subject is not None and not subject.is_disposed:
            subject.on_error(error)

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

        if not self._running:
            raise RuntimeError("client is not running")

        def factory(observer, scheduler):
            async def run():
                try:
                    await self._stream_request(req_id, msg, req_subject)
                except asyncio.CancelledError:
                    # already handled inside _stream_request; do not double-report
                    raise
                except Exception as e:
                    if not req_subject.is_disposed:
                        req_subject.on_error(e)
                finally:
                    # release strong ref to prevent Task accumulation
                    self._pending_tasks.discard(task_ref[0])

            # avoid data loss
            req_subject.subscribe(observer)

            # ==============================================================
            # Dynamic Thread Detection
            # ==============================================================
            if self.loop is None:
                req_subject.on_error(RuntimeError("no event loop attached to AsyncRpcClient"))
                return

            task_ref = [None]  # mutable holder so run() can access the task
            fut_ref = [None]   # concurrent Future from run_coroutine_threadsafe

            def teardown():
                # Subscription disposed (consumer cancelled / timed out / errored,
                # or completed normally): stop the in-flight request instead of
                # letting it stream into the void until the server closes.
                # No-op when the task already finished.
                try:
                    task = task_ref[0]
                    if task is not None:
                        self.loop.call_soon_threadsafe(task.cancel)
                    elif fut_ref[0] is not None:
                        fut_ref[0].cancel()
                except RuntimeError:
                    pass  # loop already closed: nothing left to cancel onto

            try:
                curr_loop = asyncio.get_running_loop()
                if curr_loop is self.loop:
                    # Case 1: pytest
                    task = self.loop.create_task(run())
                    task_ref[0] = task
                    self._pending_tasks.add(task)
                else:
                    # Case 2: difference loop
                    fut_ref[0] = asyncio.run_coroutine_threadsafe(run(), self.loop)

            except RuntimeError:
                # Case 3: Cerebro  prepare mdapi
                fut_ref[0] = asyncio.run_coroutine_threadsafe(run(), self.loop)

            return teardown

        observable = reactivex.create(factory)
        return (observable, req_subject)

    async def direct_run_async(self, bytes req_id, object msg):
        await self._ensure_connection()
        cdef list buffer = []
        try:
            response_iterator = self.rpc_client.on_request(msg.topic, msg.body)
            async for payload in response_iterator:
                if payload is not None:
                    buffer.append(payload)
            return buffer
        except grpc.aio.AioRpcError as e:
            print(f"[gRPC Direct Error] Code: {e.code()}")
            raise e
        except Exception as e:
            print(f"[gRPC Direct Unknown Error] {e}")
            raise e
