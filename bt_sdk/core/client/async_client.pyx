#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: language_level=3, boundscheck=False, wraparound=False

import os
import socket
import asyncio
import uvloop
import reactivex
import uuid
import zmq
import zmq.asyncio
import threading
import pyarrow as pa
import pyarrow.compute as pc
import reactivex.operators as ops
import grpc
from reactivex import of
from reactivex.subject import Subject
from reactivex.scheduler.eventloop import AsyncIOScheduler
from concurrent.futures import Future, ThreadPoolExecutor

from bt_sdk.core.protocol import _ENCODER, _RespDECODER
from bt_sdk.core.rpc.client cimport RpcClient


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

cdef int LENGTH_BYTES = 4
cdef int REQ_ID_SIZE = 16


cpdef object scale(dict data):
    """
        Table / RecordBatch scale
    """
    cdef object table = data["data"]
    cdef list columns = []
    cdef dict scale_configs = {
        **{col: 1e-5 for col in ["open", "high", "low", "close"]}, 
        **{col: 1e-3 for col in ["volume", "amount", "bonus_share", "transfer", "bonus", "price", "ratio"]}
    }
    cdef list field_names = table.schema.names
    cdef object col_data

    if table is None: return None
    
    for name in field_names:
        col_data = table.column(name) 
        if name in scale_configs:
            factor = scale_configs[name]
            col_data = pc.round(pc.multiply(col_data, factor), ndigits=2) # vectorize on C++ better than divide
        
        columns.append(col_data)
    return pa.Table.from_arrays(columns, names=field_names) # pa.RecordBatch.from_arrays


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
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                loop.create_task(self._async_shutdown())
        except RuntimeError:
            print("Close RuntimeError")
            pass # No loop running, nothing to clean up

        print(f"[{self.__class__.__name__}] Shutdown complete.")


cdef class AsyncStreamClient(AsyncClient):
    
    def __init__(self, tuple addr, int timeout=30):
        super().__init__()
            
        self.host, self.port = addr
        self._initialized = False

        self._conn_lock = asyncio.Lock() 
        self._connection_cache = {}
        self._req_futures = {}
        self.timeout = timeout
        self.listen_task = None
        self.loop = None
        self._background_tasks = set() # to ref asyncio task avoid gc
        self._bridge_tasks = set() # to ref future task void gc

    cpdef void attach_loop(self, loop, bint is_background=False):
        self.loop = loop
        self.is_background_loop = is_background
        print(f"[{self.__class__.__name__}] Attached Loop: {id(loop)} (Background: {is_background})")
    
    async def _ensure_initialized(self):
        """
            Initialize Loop、Lock、Listen Task
        """
        if self._initialized:
            return
        try:
            if self._conn_lock is None:
                self._conn_lock = asyncio.Lock()
            
            if self.listen_task is None or self.listen_task.done():
                print(f"[TCP] Starting listener loop on {self.host}:{self.port} (Loop: {id(self.loop)})")
                self.listen_task = self.loop.create_task(self._listen_loop())

            self._initialized = True
        except Exception as e:
            print(f"[TCP Init Error] {e}")
            raise  

    async def _get_connection(self, str connection_key):
        cdef object reader, writer

        async with self._conn_lock:
            if connection_key in self._connection_cache:
                reader, writer = self._connection_cache[connection_key]
                if not writer.is_closing():
                    return reader, writer
                else:
                    del self._connection_cache[connection_key]
        
            print(f"[TCP] Connecting to {self.host}:{self.port}...")
            try: 
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port), 
                    timeout=self.timeout
                )
                sock = writer.get_extra_info('socket') # disable Nagle to decrease dalay due to small packed
                if sock:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                self._connection_cache[connection_key] = (reader, writer)
                print(f"[TCP] Connected successfully.")
                return reader, writer
            except Exception as e:
                 raise ConnectionError(f"Failed to connect to {self.host}:{self.port}: {e}")

    async def _listen_loop(self):
        cdef bytes raw_payload
        cdef list payload
        cdef bytes r_id
        cdef str connection_key = f"{self.host}:{self.port}"
        cdef object reader = None, writer = None
        
        await self._ensure_initialized()

        print(f"[TCP] Reader for {connection_key} started.")
        while self._running:
            try:
                if reader is None or writer is None or writer.is_closing():  # reader.at_eof() means close connect
                    reader, writer = await self._get_connection(connection_key)

                len_bytes = await reader.readexactly(LENGTH_BYTES) # await asyncio.wait_for(reader.readexactly(LENGTH_BYTES), timeout=30.0)
                msg_len = int.from_bytes(len_bytes, 'big')

                if msg_len == 0: continue # Heartbeat
                
                if msg_len > 10 * 1024 * 1024:
                    raise ValueError("Packet too large")

                complete_message = await reader.readexactly(msg_len)
                req_id = complete_message[:REQ_ID_SIZE]
                payload = _RespDECODER.decode(complete_message[REQ_ID_SIZE:])
                
                fut = self._req_futures.pop(req_id, None)
                if fut and not fut.done():
                    fut.set_result(payload)# ensure cross thread safely / fut.set_result(payload)

            except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError, OSError) as e:
                if self._running:
                    print(f"[TCP] Connection lost: {e}. Cleaning up and retrying...")
                    async with self._conn_lock: 
                        self._connection_cache.pop(connection_key, None) # clean cache avoid send_request reuse
                    
                    if writer:
                        writer.close() # nonblock send Fin and stop writer 
                    reader = None
                    writer = None  
                    await asyncio.sleep(1) 
            except Exception as e:
                print(f"[TCP] Unexpected error: {e}")
                await asyncio.sleep(1)

    cdef object wrap_protocol(self, bytes req_id, object msg): 
        cdef object fut

        if self.loop is None:
             raise RuntimeError("Loop not attached! Call attach_loop() first.")
        
        if self.is_background_loop:
            fut = Future() # sync 
            
            async def bridge():
                current_task = asyncio.current_task()
                self._bridge_tasks.add(current_task)
                try:
                    self._req_futures[req_id] = fut 
                    await self.send_request(req_id, msg)
                except Exception as e:
                    fut.set_exception(e)
                finally:
                    self._bridge_tasks.discard(current_task)

            asyncio.run_coroutine_threadsafe(bridge(), self.loop)
        else:
            fut = self.loop.create_future() # Ray / Async
            self._req_futures[req_id] = fut
            
            task = self.loop.create_task(self.send_request(req_id, msg))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        return fut 

    async def send_request(self, bytes req_id, object msg):
        cdef bytes body = _ENCODER.encode(msg)
        cdef int total_length = REQ_ID_SIZE + len(body) 
        cdef str connection_key = f"{self.host}:{self.port}"

        await self._ensure_initialized()

        reader, writer = await self._get_connection(connection_key) 
        writer.writelines([
            total_length.to_bytes(LENGTH_BYTES, 'big'),
            req_id,
            body
        ])
        await writer.drain()

    async def _async_shutdown(self):
        if self._connection_cache:
            for key in list(self._connection_cache.keys()):
                reader, writer = self._connection_cache.pop(key)
                if writer:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception as e:
                        pass
            self._connection_cache.clear()

        if self._req_futures:
            for req_id, fut in self._req_futures.items():
                if not fut.done():
                    fut.cancel()
            self._req_futures.clear()

        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"[TCP] Listen Task error: {e}")
        self.listen_task = None

        try:
            current_task = asyncio.current_task()
            all_tasks = asyncio.all_tasks(loop=self.loop)
            pending_tasks = [t for t in all_tasks if t is not current_task and not t.done()]
            
            if pending_tasks:
                for task in pending_tasks:
                    task.cancel()
                await asyncio.gather(*pending_tasks, return_exceptions=True)
        except Exception as e:
            print(f"[TCP] Error cleaning pending tasks: {e}")
        print("[TCP] Shutdown complete.")


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
        
    cpdef void attach_loop(self, loop, bint is_background=False):
        self.loop = loop
        self.is_background_loop = is_background
        print(f"[{self.__class__.__name__}] Attached Loop: {id(loop)} (Background: {is_background})")
        
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

    cdef object wrap_protocol(self, bytes req_id, object msg):
        cdef object req_subject = Subject() # cold mode avoid hot mode (data into hole when self._stream_request)

        if not self._running:
            raise RuntimeError("client is not running")

        def factory(observer, scheduler):
            async def run():
                try:
                    await self._stream_request(req_id, msg, req_subject)
                except Exception as e:
                    req_subject.on_error(e)

            if self.is_background_loop:
                future = asyncio.run_coroutine_threadsafe(run(), self.loop)
                future.add_done_callback(lambda f: self._finalize_task(f))
            else:
                self.loop.create_task(run())

            req_subject.subscribe(observer)

        observable = reactivex.create(factory)
        return observable.pipe(
            # ops.sample(0.1),  # 100ms abandon reset 
            # ops.buffer_with_time_or_count(timespan=1.0, count=500), # up to 500 / 1 second to list
            # ops.throttle_first(0.05), # on receive / 50ms not receive
            # ops.publish_replay(1), # cache 1 record 
            # ops.ref_count()
            ops.map(scale),
            ops.share()
        )

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
                print(f"[gRPC Error] Code: {e.code()}, Details: {e.details()}")
                subject.on_error(e)
            except asyncio.CancelledError:
                print("[gRPC] Request Cancelled")
                subject.on_completed()
            except Exception as e:
                print(f"[gRPC Unknown Error] {e}")
                subject.on_error(e)
