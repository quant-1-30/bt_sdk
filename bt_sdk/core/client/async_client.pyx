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

    cpdef void close(self):
        if not self._running: return
        self._running = False
        loop = self.loop
        try:
            if loop is None:
                loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(self._async_shutdown(), loop)
                fut.result(timeout=3)
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

        loop = self.loop
        if loop is not None and not loop.is_closed() and loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(self.rpc_client.cleanup(), loop)
                fut.result(timeout=self.timeout)
            except Exception as e:
                logger.warning(f"[{self.__class__.__name__}] reset_connection cleanup error: {e}")
        else:
            self.rpc_client._channel = None
            self.rpc_client._stub = None

        print(f"[{self.__class__.__name__}] Connection reset for new loop")

    async def _async_shutdown(self):
        """覆盖基类：先取消 listen_task，再关闭 gRPC channel。"""
        await super()._async_shutdown()
        # 关闭 gRPC channel，防止资源泄漏
        try:
            await self.rpc_client.cleanup()
        except Exception as e:
            logger.warning(f"[{self.__class__.__name__}] gRPC cleanup error: {e}")
        self._connected = False

    async def _ensure_connection(self):
        """
        Lazy initialize gRPC channel
        """
        if self._connected:
            return
        try:
            print(f"[gRPC] Initializing Channel on Loop: {id(self.loop)}")
            await self.rpc_client.initialize()
            self._connected = True
        except Exception as e:
            print(f"[gRPC Init Error] {e}")
            raise e

    async def _stream_request(self, bytes req_id, object msg, object subject):
            await self._ensure_connection()
            try:
                response_iterator = self.rpc_client.on_request(msg.topic, msg.body)

                async for payload in response_iterator: # pyarrow.lib.Table
                    if payload is not None:
                        subject.on_next({
                            "id": req_id,  
                            "data": payload
                        })
                subject.on_completed()
            except grpc.aio.AioRpcError as e:
                logger.error(f"[gRPC Error] Code: {e.code()}, Details: {e.details()}")
                subject.on_error(e)
            except asyncio.CancelledError:
                logger.info("[gRPC] Request Cancelled")
                # propagate cancellation while keeping subject consistent
                subject.on_completed()
                raise
            except Exception as e:
                logger.exception(f"[gRPC Unknown Error] {e}")
                subject.on_error(e)

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

            # avoid data loss
            req_subject.subscribe(observer)

            # ==============================================================
            # Dynamic Thread Detection
            # ==============================================================
            if self.loop is None:
                req_subject.on_error(RuntimeError("no event loop attached to AsyncRpcClient"))
                return

            try:
                curr_loop = asyncio.get_running_loop()
                if curr_loop is self.loop:
                    # Case 1: pytest
                    self.loop.create_task(run())
                else:
                    # Case 2: difference loop
                    asyncio.run_coroutine_threadsafe(run(), self.loop)
                    
            except RuntimeError:
                # Case 3: Cerebro  prepare mdapi 
                asyncio.run_coroutine_threadsafe(run(), self.loop)

        observable = reactivex.create(factory)
        return observable

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
