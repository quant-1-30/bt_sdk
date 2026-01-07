#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: language_level=3, boundscheck=False, wraparound=False

import os
import json
import time
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
from reactivex import of
from reactivex.subject import Subject
from reactivex.scheduler.eventloop import AsyncIOScheduler
from concurrent.futures import ThreadPoolExecutor

from core.protocol import _ENCODER, _DECODER


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

cdef int LENGTH_BYTES = 4
cdef int REQ_ID_SIZE = 16


cdef inline object _deserialize_to_table(bytes arrow_bytes): # inline function embed to reduce overhead when hundrends
    if not arrow_bytes:
        return None   
    # pa.py_buffer to wrap Python bytes
    # open_stream zero_copy parse 
    # read_all() ---> Table (underlying data point ZMQ bytes 
    table = pa.ipc.open_stream(pa.py_buffer(arrow_bytes)).read_all()
    # pc zero_copy and vectorize
    for col_name in ["sid", "name"]:
        if col_name in table.column_names:
            col = table.column(col_name)
            table = table.set_column(
                table.column_names.index(col_name),
                col_name,
                pc.cast(col, pa.string())
            )
    return table


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

    # rebuild once
    return pa.Table.from_arrays(columns, names=field_names)
    # return pa.RecordBatch.from_arrays(columns, names=field_names)


cdef class AsyncClient:

    def __init__(self):
        
        self._req_subject = {} # self._global_bus = Subject() # global bus to filter req is heavy cpu operation when max concurrent 
        self._req_futures = {}
        self._running = True
        
        self._init_event_loop()
    
    cdef void _init_event_loop(self):
        self.loop = asyncio.new_event_loop()

        if hasattr(self.loop, 'set_debug'):
            self.loop.set_debug(False)
        
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True, # to clean up thread when main thread finish 
            name="AsyncClient-EventLoop"
        )
        self._loop_thread.start()
        
    cdef void _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        except Exception as e:
            print(f"Event loop error: {e}")

    cdef void _finalize_task(self, object future):
        try:
            ret = future.result()  # result or exception
        except Exception as e:
            print(f"任务执行失败: {e}")

    cpdef object run(self, bytes req_id, object msg):
        if not self._running:
            raise RuntimeError("client is not running")
        
        obs_or_fut = self.wrap_protocol(req_id, msg)
        return obs_or_fut

    cdef object wrap_protocol(self, bytes req_id, object msg):
        """implement on protocol"""
        pass    

    async def send_request(self, bytes req_id, dict message):
        """子类需实现具体的发送逻辑"""
        pass

    async def _async_shutdown(self):
        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()
            try:
                await self.listen_task
            except asyncio.CancelledError:
                pass

    cpdef void close(self):
        if not self._running:
            return
            
        self._running = False
        print(f"[{self.__class__.__name__}] Closing...")

        if self.loop is not None and self.loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(self._async_shutdown(), self.loop)
                fut.result(timeout=2.0) # 等待子类清理资源
            except Exception as e:
                print(f"Shutdown error: {e}")
            
            self.loop.call_soon_threadsafe(self.loop.stop)
            
            if self._loop_thread and self._loop_thread.is_alive():
                self._loop_thread.join(timeout=1.0)
            
        print(f"[{self.__class__.__name__}] Shutdown complete.")


cdef class AsyncZmqClient(AsyncClient):
    """
    High-performance asynchronous ZMQ client using a DEALER socket.
    This client is designed to communicate with a ZMQ ROUTER server.
    It maintains the singleton pattern and supports concurrent requests.
    """

    def __init__(self, tuple addr, int timeout=5):
        super().__init__()
        self.addr = f"tcp://{addr[0]}:{addr[1]}"
        self.timeout = timeout
        
        # Initialize ZMQ context and socket within the loop
        future = asyncio.run_coroutine_threadsafe(self._init_zmq(), self.loop)
        future.result() # Wait for ZMQ initialization to complete

    async def _init_zmq(self):
        """Initializes ZMQ context and socket. Must be called from within the event loop."""
        self.context = zmq.asyncio.Context()
        # self.context.set(zmq.IO_THREADS, 4) # 
        self.socket = self.context.socket(zmq.DEALER)
        
        # Set a unique identity for this client for easier debugging on the server when reconnect
        client_id = f"client-{uuid.uuid4()}".encode('utf-8') # if not set client will generate uuid as client_id
        print("client identity :", client_id)
        self.socket.setsockopt(zmq.IDENTITY, client_id)

        # # zmq.SNDBUF` / `zmq.RCVBUF` kernal TCP Window 
        # self.socket.setsockopt(zmq.SNDBUF, 2*1024*1024)
        # self.socket.setsockopt(zmq.RCVBUF, 2*1024*1024)

        self.socket.setsockopt(zmq.LINGER, 0)
        # No Nagle
        self.socket.setsockopt(42, 1) # zmq.constants.TCP_NODELAY / socket.TCP_NODELAY 1
        # Set high-water mark to prevent excessive memory usage
        self.socket.set_hwm(1000)

        # # heartbeat
        # self.socket.setsockopt(zmq.HEARTBEAT_IVL, 5000)      
        # self.socket.setsockopt(zmq.HEARTBEAT_TIMEOUT, 15000) 
        # self.socket.setsockopt(zmq.HEARTBEAT_TTL, 15000) 
        
        print(f"[ZMQ] Connecting to server at {self.addr}")
        self.socket.connect(self.addr)
        self.listen_task = self.loop.create_task(self._listen_loop())

    async def _listen_loop(self):
            """
            Sends a request via the ZMQ DEALER socket and asynchronously yields responses
            """
            cdef bytes payload
            cdef bytes r_id
            cdef object req_subject
            
            print("[ZMQ] Listener loop started.")
            while self._running:
                try:
                    # multi_frame [request_id, payload_bytes]
                    frames = await self.socket.recv_multipart()
                    if len(frames) < 2:
                        continue
                    
                    r_id = frames[0]
                    payload = frames[1]
                    req_subject = self._req_subject[r_id]
                    
                    if payload == b"eof":
                        req_subject.on_completed() # high efficient avoid take_while --- lambda 
                        self._req_subject.pop(r_id, None)
                        continue

                    # Table Zero-copy
                    table = _deserialize_to_table(payload)
                    
                    if table is not None:
                        req_subject.on_next({
                            "id": r_id, 
                            "data": table,
                            # "meta": table.schema.metadata 
                        })
                except zmq.ZMQError as ze:
                    if ze.errno == zmq.ETERM: break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    break 

    cdef object wrap_protocol(self, bytes req_id, object msg):
        cdef object req_subject = Subject()

        if not self._running:
            raise RuntimeError("client is not running")

        self._req_subject[req_id] = req_subject # avoid to ops.filter(lambda) --- global bus

        observable = req_subject.pipe(
            # ops.sample(0.1),  # 100ms abandon reset 
            # ops.buffer_with_time_or_count(timespan=1.0, count=500), # up to 500 / 1 second to list
            # ops.throttle_first(0.05), # on receive / 50ms not receive
            # ops.publish_replay(1), # cache 1 record 
            # ops.ref_count()
            ops.map(scale), # avoid lambda function due to python overhead and function must cpdef or def
            ops.share() 
        ) 
        coro = self.send_request(req_id, msg) 
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        future.add_done_callback(lambda f: self._finalize_task(f))
        return observable 

    async def send_request(self, bytes req_id, object msg):
        # serialize_msg = pack(msg)
        serialize_msg = _ENCODER.encode(msg)
        await self.socket.send_multipart([req_id, serialize_msg]) # multi_frame
        print("send multiframe :", req_id, serialize_msg)

    async def _async_shutdown(self):
        await AsyncClient._async_shutdown(self)

        if self.socket is not None:
            self.socket.close(linger=0) # drop immediately
            self.socket = None 

        if self.context is not None:
            self.context.term()
            self.context = None


cdef class AsyncStreamClient(AsyncClient):
    
    def __init__(self, tuple addr, int timeout=5):
        super().__init__()
            
        self.host, self.port = addr
        self._conn_lock = asyncio.Lock()  # in different coro maybe cause cache bug
        self._connection_cache = {}
        self.timeout = timeout
        
        self.listen_task = self.loop.create_task(self._listen_loop())

    async def _listen_loop(self):
        cdef bytes raw_payload
        cdef list payload
        cdef bytes r_id
        cdef str connection_key = f"{self.host}:{self.port}"

        reader, writer = await self._get_connection(connection_key) # cache            

        print(f"[TCP] Reader for {connection_key} started.")

        # while self._running and not reader.at_eof():
        while self._running:
            try:
                len_bytes = await asyncio.wait_for(reader.readexactly(LENGTH_BYTES), timeout=30.0)
                msg_len = int.from_bytes(len_bytes, 'big')

                if msg_len == 0:
                    continue # reset wait_for timeout
                
                if msg_len > 10 * 1024 * 1024: 
                    raise ValueError("Packet too large")

                complete_message = await reader.readexactly(msg_len)
                req_id = complete_message[:REQ_ID_SIZE] 
                # payload = unpack(complete_message[REQ_ID_SIZE:]) 
                payload = _DECODER.decode(complete_message[REQ_ID_SIZE:]) 
                
                fut = self._req_futures.pop(req_id, None)
                if fut and not fut.done():
                    fut.set_result(payload)
            except asyncio.TimeoutError:
                print("TCP timeout")
                break
            except ConnectionError: # asyncio.IncompleteReadError 
                print(f"TCP ConnectionError")
                break
            except Exception as e:
                print(f"[TCP] Listen_loop error: {e}")
                break
            
    cdef object wrap_protocol(self, bytes req_id, object msg): # return future to user to determin await or result block
        """TCP 返回 Coroutine 用户需 await"""
        from concurrent.futures import Future
        cdef object fut = Future() # block 
        # fut = self.loop.create_future() # nonblock 
        self._req_futures[req_id] = fut 
        
        asyncio.run_coroutine_threadsafe(self.send_request(req_id, msg), self.loop)
        return fut # await asyncio.wrap_future(fut) to transform block to nonblock

    async def send_request(self, bytes req_id, object msg):
        connection_key = f"{self.host}:{self.port}"
        reader, writer = await self._get_connection(connection_key) # cache            
        
        # serialize_msg = req_id + pack(msg) # req_id 16 bytes
        serialize_msg = req_id + _ENCODER.encode(msg) # req_id 16 bytes
        msg_len = len(serialize_msg)
        writer.write(msg_len.to_bytes(LENGTH_BYTES, byteorder='big') + serialize_msg)
        await writer.drain()

    async def _get_connection(self, str connection_key):
        """
            获取或创建连接确保每个连接有且只有一个后台读取任务
        """
        async with self._conn_lock:
            if connection_key in self._connection_cache:
                reader, writer = self._connection_cache[connection_key]
                if not writer.is_closing():
                    return reader, writer
                else:
                    del self._connection_cache[connection_key]
        
            reader, writer = await asyncio.open_connection(host=self.host, port=self.port) # same underlying File Descriptor/Socket
            # socketopt No Nagle when small packet 
            sock = writer.get_extra_info('socket')
            if sock:
                import socket
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            self._connection_cache[connection_key] = (reader, writer)
            return reader, writer

    async def _async_shutdown(self):
        await AsyncClient._async_shutdown()

        for key in list(self._connection_cache.keys()):
            reader, writer = self._connection_cache.pop(key)
            if writer:
                writer.close()
                try:
                    # await writer.wait_closed()
                    await asyncio.wait_for(writer.wait_closed(), timeout=1.0) # writer.close()
                except: pass
        self._connection_cache.clear()
        print("[TCP] All connections closed.")

    