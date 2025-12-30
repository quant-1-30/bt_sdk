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
import reactivex.operators as ops
from reactivex import of
from reactivex.subject import Subject
from reactivex.scheduler.eventloop import AsyncIOScheduler
from concurrent.futures import ThreadPoolExecutor

from utils.serialize import pack, unpack

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

cdef int LENGTH_BYTES = 4
cdef int REQ_ID_SIZE = 16


cdef inline _deserialize_to_table(bytes arrow_bytes):
    if not arrow_bytes:
        return None
        
    # pa.py_buffer to wrap Python bytes
    # open_stream zero_copy parse 
    # read_all() ---> Table (underlying data point ZMQ bytes 
    try:
        return pa.ipc.open_stream(pa.py_buffer(arrow_bytes)).read_all()
    except Exception as e:
        print(f"Arrow deserialization failed: {e}")
        return None


cdef class AsyncClient:

    def __init__(self):
        
        self._global_bus = Subject() # global accumlate 
        self._running = True
        
        self._init_event_loop()
    
    cdef void _init_event_loop(self):
        self.loop = asyncio.new_event_loop()

        if hasattr(self.loop, 'set_debug'):
            self.loop.set_debug(False)
        
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
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

    def run(self, bytes req_id, dict msg):
        # cython not support lambda or nested
            if not self._running:
                raise RuntimeError("client is not running")

            cdef object observable = self._global_bus.pipe(
                ops.filter(lambda x: x.get("id") == req_id),
                ops.take_while(lambda x: x.get("data") != "eof"),
                ops.map(lambda x: x.get("data"))
            )

            # only send rq and receive api move to subclass and accumlate to global bus
            coro = self.send_request(req_id, msg) 
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            future.add_done_callback(lambda f: self._finalize_task(f))
            return observable

    async def send_request(self, bytes req_id, dict message):
        """子类需实现具体的发送逻辑"""
        pass

    cpdef void close(self):
        # Stop the event loop itself
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._loop_thread.join(timeout=2)
            print("Client Loop shutdown complete.")   


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
        self.socket = self.context.socket(zmq.DEALER)
        
        # Set a unique identity for this client for easier debugging on the server when reconnect
        client_id = f"client-{uuid.uuid4()}".encode('utf-8')
        self.socket.setsockopt(zmq.IDENTITY, client_id)

        # # zmq.SNDBUF` / `zmq.RCVBUF` kernal TCP Window 
        # self.socket.setsockopt(zmq.SNDBUF, 2*1024*1024)
        # self.socket.setsockopt(zmq.RCVBUF, 2*1024*1024)

        # No Nagle
        self.socket.setsockopt(zmq.TCP_NODELAY, 1) 
        # Set high-water mark to prevent excessive memory usage
        self.socket.set_hwm(1000)
        
        print(f"[ZMQ] Connecting to server at {self.addr}")
        self.socket.connect(self.addr)

        self.loop.create_task(self._listen_loop())

    async def _listen_loop(self):
            """
            Sends a request via the ZMQ DEALER socket and asynchronously yields responses
            """
            cdef bytes raw_payload
            cdef dict payload
            cdef bytes r_id
            cdef object bus_on_next = self._global_bus.on_next 
            
            print("[ZMQ] Listener loop started.")
            while self._running:
                try:
                    # recv_multipart multi_frame [request_id, payload_bytes]
                    frames = await self.socket.recv_multipart()
                    
                    if len(frames) < 2:
                        continue
                    
                    r_id = frames[0]
                    raw_payload = frames[1]
                    payload = unpack(raw_payload)
                    
                    if payload == b"eof":
                        bus_on_next({"id": r_id, "data": b"eof"})
                        continue

                    # Table Zero-copy
                    table = _deserialize_to_table(payload)
                    
                    if table is not None:
                        bus_on_next({
                            "id": r_id, 
                            "data": table,
                            # "meta": table.schema.metadata 
                        })
                except zmq.ZMQError as ze:
                    if ze.errno == zmq.ETERM: break
                except Exception as e:
                    print(f"[ZMQ] Processing Error: {e}") 

    async def send_request(self, bytes req_id, dict message):
        serialize_msg = pack(message)
        # multi_frame [req_id, serialized_msg]
        await self.socket.send_multipart([req_id, serialize_msg])

    cpdef void close(self): # cython no supported nested function
        """Gracefully shuts down the client."""
        if not self._running:
            return
        
        print("[ZMQ] Shutting down client...")
        self._running = False

        # Schedule the cleanup on the event loop
        self.loop.call_soon_threadsafe(self.shutdown_async_resources)
        super().close()

    cdef void shutdown_async_resources(self):
        if hasattr(self, 'socket'):
            self.socket.close()
        if hasattr(self, 'context'):
            self.context.term()


cdef class AsyncStreamClient(AsyncClient):
    
    def __init__(self, tuple addr, int timeout=5):
        super().__init__()
            
        self.host, self.port = addr
        self._connection_cache = {} 
        self.timeout = timeout
        
        self.loop.create_task(self._listen_loop())

    async def _listen_loop(self):
        cdef bytes raw_payload
        cdef dict payload
        cdef bytes r_id
        cdef object bus_on_next = self._global_bus.on_next 
        cdef str connection_key = f"{self.host}:{self.port}"

        reader, writer = await self._get_connection(connection_key) # cache            

        print(f"[TCP] Reader for {connection_key} started.")

        while self._running and not reader.at_eof():
            try:
                len_bytes = await reader.readexactly(LENGTH_BYTES)
                msg_len = int.from_bytes(len_bytes, 'big')

                complete_message = await reader.readexactly(msg_len)
                req_id = complete_message[:REQ_ID_SIZE] 

                if msg_len == 16: 
                    bus_on_next({"id": req_id, "data": b"eof"})
                    continue 

                payload = unpack(complete_message[REQ_ID_SIZE:]) # pyarrow 优化
                print("tcp recv payload :", payload)

                bus_on_next({"id": req_id, "data": payload})

            except asyncio.IncompleteReadError:
                print(f"[TCP] Connection lost for {connection_key}")
                await asyncio.sleep(1) 
            except ConnectionError:
                await asyncio.sleep(1)
            except Exception as e:
                print(f"[TCP] Process error: {e}")
            
    async def _get_connection(self, str connection_key):
        """
            获取或创建连接确保每个连接有且只有一个后台读取任务
        """
        if connection_key in self._connection_cache:
            reader, writer = self._connection_cache[connection_key]
            if not writer.is_closing():
                return reader, writer
            else:
                del self._connection_cache[connection_key]
        
        reader, writer = await asyncio.open_connection(host=self.host, port=self.port) # same underlying File Descriptor/Socket
        # socketopt
        sock = writer.get_extra_info('socket')
        if sock:
            # No Nagle when small packet
            import socket
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
        self._connection_cache[connection_key] = (reader, writer)
        return reader, writer

    async def send_request(self, bytes req_id, dict message):
        connection_key = f"{self.host}:{self.port}"
        reader, writer = await self._get_connection(connection_key) # cache            
        
        serialize_msg = req_id + pack(message) # req_id 16 bytes
        msg_len = len(serialize_msg)
        writer.write(msg_len.to_bytes(LENGTH_BYTES, byteorder='big') + serialize_msg)
        await writer.drain()

    cpdef void close(self):
        """Gracefully shuts down the client."""
        if not self._running:
            return
        
        print("[TCP] Shutting down client...")
        self._running = False

        if hasattr(self, 'loop') and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._async_close(), self.loop)
            try:
                future.result(timeout=5.0)
            except Exception as e:
                print(f"Error during client shutdown: {e}")
        super().close()
    
    async def _async_close(self):
            """符合 asyncio 标准的优雅关闭"""
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
