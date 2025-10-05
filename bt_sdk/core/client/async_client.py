#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import asyncio
import threading
import uuid
import zmq
import zmq.asyncio
from queue import Queue
from typing import Dict, Any, Set, Optional
from concurrent.futures import ThreadPoolExecutor

from bt_sdk.utils.serialize import pack, unpack


class AsyncClient:
    """
    高性能异步客户端基类。
    修改：增加了对并发请求的管理机制。
    """

    _instance = None
    _initialization_complete = False
    _lock_instance = threading.RLock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock_instance:
                if cls._instance is None:
                    cls._instance = super(AsyncClient, cls).__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
                    cls._initialization_complete = True
        return cls._instance
    
    def _initialize(self, *args, **kwargs):
        if hasattr(self, '_initialized'):
            return
        
        self._initialized = True
        self._running = True
        self.buffer_size = int(kwargs.get("buffer_size", 65536))  # 增加默认缓冲区以应对并发
        
        self._tasks = set()
        
        self._pending_requests: Dict[str, asyncio.Queue] = {}
        
        self._executor = ThreadPoolExecutor(
            max_workers=min(4, os.cpu_count() or 1),
            thread_name_prefix="AsyncClient"
        )
        
        self._init_event_loop()
    
    def _init_event_loop(self):
        """优化的事件循环初始化"""
        self.loop = asyncio.new_event_loop()

        # # 在事件循环线程中创建需要绑定到循环的资源
        # def init_loop_resources():
        #     # self._async_reader_lock 不再需要在多个协程间共享读取，因此可以移除
        #     pass
        
        # future = self.loop.run_in_executor(None, init_loop_resources)
        # self.loop.run_until_complete(future)

        if hasattr(self.loop, 'set_debug'):
            self.loop.set_debug(False)
        
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="AsyncClient-EventLoop"
        )
        self._loop_thread.start()
        
        timeout = 5.0
        start_time = time.time()
        while not self.loop.is_running() and (time.time() - start_time) < timeout:
            time.sleep(0.01)
    
    def _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        print("[loop] Event loop started")
        try:
            self.loop.run_forever()
        except Exception as e:
            print(f"Event loop error: {e}")
        finally:
            print("[loop] Event loop stopped")

    def run(self, msg, msg_q):
       
        if not self._running:
            self._ensure_eof(msg_q) # 创建结束事件，避免依赖队列EOF
            return
        try:
            coro = self.on_receive(msg, msg_q)
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            self._tasks.add(future)
            
            def cleanup_callback(f):
                try:
                    f.result()
                except Exception as e:
                    print(f"[CALLBACK ERROR] {e}")
                finally:
                    self._tasks.discard(f)
                    self._ensure_eof(msg_q)

            future.add_done_callback(cleanup_callback)
            
        except Exception as e:
            print(f"Error starting task: {e}")
            self._ensure_eof(msg_q)
    
    async def on_receive(self, message: Dict[str, Any], msg_q: Queue):
        """优化的消息接收处理 - 支持结束事件"""
        task = asyncio.current_task()
        try:
            data_count = 0
            async for data in self.get_data(message):
                msg_q.put(data)
                data_count += 1

                if data == "eof":
                    break
                    
                if data_count % 100 == 0: # 批量处理优化：每100条消息让出控制权
                    await asyncio.sleep(0)
                    
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Error in on_receive: {e}")
        finally: # 不管try or except 存在return / raise 都会执行
            if hasattr(self, '_tasks') and task:
                self._tasks.discard(task)
    
    def _ensure_eof(self, req_q):
        
        def cb():
            max_attempts = 3
            attempt = 0
            base_delay = 0.001
            
            while attempt < max_attempts:
                try:
                    if hasattr(req_q, 'put_nowait'):
                        req_q.put_nowait("eof")
                        return True
                except Exception:
                    pass
                
                delay = min(base_delay * (attempt + 1), 0.01)
                time.sleep(delay)
                attempt += 1
            return False
        
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Loop already closed")
            loop.call_soon_threadsafe(cb)
        except Exception as e:
            thread = threading.Thread(target=cb, daemon=True)
            thread.start()


class AsyncStreamClient(AsyncClient):
    """
    优化的TCP客户端 - 重构以支持在单一连接上安全地进行请求复用
    """
    def __init__(self, addr, timeout=5.0):
        if not self._initialization_complete:
            super()._initialize()
            
        self.host, self.port = addr
        # reader, writer, reader_task
        self._connection_cache: Dict[str, tuple] = {} 
        self.LENGTH_BYTES = 4
        self.timeout = timeout

    async def _receive_loop(self, reader: asyncio.StreamReader, connection_key: str):
        """
            TCP连接专属的后台读取任务。
        """
        print(f"[TCP] Reader for {connection_key} started.")
        while self._running and not reader.at_eof():
            try:
                # 读取消息长度
                length_bytes = await reader.readexactly(self.LENGTH_BYTES)
                msg_len = int.from_bytes(length_bytes, 'big')
                if msg_len == 0: continue

                # 读取完整消息体
                complete_message = await reader.readexactly(msg_len)

                # 解包并分发
                request_id, payload = unpack(complete_message)
                # print("payload :", payload)

                if request_id in self._pending_requests:
                    await self._pending_requests[request_id].put(payload)
                else:
                    print(f"[TCP Warning] Received message for unknown request_id: {request_id}")

            except (asyncio.IncompleteReadError, ConnectionResetError): # retry logic
                print(f"[TCP] Connection {connection_key} closed.")
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[TCP Reader Error] for {connection_key}: {e}")
                break
        
        print(f"[TCP] Reader for {connection_key} stopped.")
        
        if connection_key in self._connection_cache:
            _, writer, _ = self._connection_cache[connection_key]
            await self._close_connection(writer, connection_key)

    async def _get_connection(self, connection_key):
        """
            获取或创建连接，并确保每个连接有且只有一个后台读取任务
        """
        if connection_key in self._connection_cache:
            reader, writer, reader_task = self._connection_cache[connection_key]
            if not writer.is_closing() and not reader_task.done():
                return reader, writer
            else:
                # 清理失效的连接和任务
                reader_task.cancel()
                del self._connection_cache[connection_key]
        
        reader, writer = await asyncio.open_connection(host=self.host, port=self.port)
        reader_task = asyncio.create_task(self._receive_loop(reader, connection_key))
        
        self._connection_cache[connection_key] = (reader, writer, reader_task)
        return reader, writer

    async def get_data(self, message):
        """
            通过请求ID在共享的TCP连接上安全地发送和接收数据。
        """
        # request_id = str(uuid.uuid4())
        request_id=message["request_id"]
        response_queue = asyncio.Queue()
        self._pending_requests[request_id] = response_queue
        connection_key = f"{self.host}:{self.port}"

        try:
            _, writer = await self._get_connection(connection_key) # cache
            
            # 假设 pack 函数现在接受 request_id
            # serialize_msg = pack(**message, request_id=request_id)
            serialize_msg = pack(message)
            
            msg_len = len(serialize_msg)
            writer.write(msg_len.to_bytes(self.LENGTH_BYTES, byteorder='big'))
            writer.write(serialize_msg)
            await writer.drain()

            while True:
                try:
                    data = await asyncio.wait_for(response_queue.get(), timeout=10.0)
                    # print("get_data :", data)
                    if data["body"] == "eof":
                        break
                    yield data
                
                except asyncio.TimeoutError:
                    print(f"[TCP Timeout] No response for request {request_id} within timeout period.")
                    break
            yield "eof"

        except Exception as e:
            print(f"[TCP Error] {e} for request {request_id}")
            yield "eof"

            if connection_key in self._connection_cache:
                writer = self._connection_cache[connection_key][1]
                await self._close_connection(writer, connection_key)
        finally:
            self._pending_requests.pop(request_id, None)
    
    async def _close_connection(self, writer, connection_key):
        """
        *** 修改 ***: 关闭连接时，同时取消其关联的读取任务。
        """
        if connection_key in self._connection_cache:
            _, _, reader_task = self._connection_cache.pop(connection_key)
            reader_task.cancel() # 取消后台任务
            try:
                await asyncio.wait_for(reader_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

            try:
                writer.close()
                # await writer.wait_closed()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception as e:
                print(f"Error closing connection: {e}")

    async def close_async(self):
        """
        异步关闭所有连接和资源
        """
        if not self._running:
            return
        
        print("[TCP] Shutting down client...")
        self._running = False
        
        # 取消所有pending请求
        for request_id in list(self._pending_requests.keys()):
            queue = self._pending_requests.pop(request_id, None)
            if queue:
                await queue.put({"body": "eof"})
        
        # 关闭所有连接
        connection_keys = list(self._connection_cache.keys())
        for connection_key in connection_keys:
            if connection_key in self._connection_cache:
                _, writer, _ = self._connection_cache[connection_key]
                await self._close_connection(writer, connection_key)
        
        self._connection_cache.clear()
        self._pending_requests.clear()
        print("[TCP] Client shutdown complete.")

    def close(self):
        """
        同步关闭方法（如果需要在非异步上下文中调用）
        """
        if not self._running:
            return
        
        # 在事件循环中运行异步关闭
        if hasattr(self, 'loop') and self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self.close_async(), self.loop)
            try:
                future.result(timeout=5.0)
            except Exception as e:
                print(f"Error during client shutdown: {e}")
        else:
            # 如果没有运行的事件循环，直接运行
            asyncio.run(self.close_async())


class AsyncZmqClient(AsyncClient):
    """
    High-performance asynchronous ZMQ client using a DEALER socket.
    This client is designed to communicate with a ZMQ ROUTER server.
    It maintains the singleton pattern and supports concurrent requests.
    """

    def __init__(self, addr, timeout=5.0):
        if not self._initialization_complete:
            super()._initialize()

        self.addr = addr
        self.timeout = timeout
        
        # Initialize ZMQ context and socket within the loop
        future = asyncio.run_coroutine_threadsafe(self._init_zmq(), self.loop)
        future.result() # Wait for ZMQ initialization to complete

    async def _init_zmq(self):
        """Initializes ZMQ context and socket. Must be called from within the event loop."""
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.DEALER)
        
        # Set a unique identity for this client for easier debugging on the server
        client_id = f"client-{uuid.uuid4()}".encode('utf-8')
        self.socket.setsockopt(zmq.IDENTITY, client_id)
        
        # Set high-water mark to prevent excessive memory usage
        self.socket.set_hwm(1000)
        
        print(f"[ZMQ] Connecting to server at {self.addr}")
        self.socket.connect(self.addr)
        
        # Start the background task to listen for all incoming messages
        self._receive_task = self.loop.create_task(self._receive_loop())

    async def _receive_loop(self):
        """
        A single, continuous background task that receives all messages from the ZMQ socket
        and dispatches them to the correct pending request queue based on request_id.
        """
        print("[ZMQ] Receiver loop started.")
        while self._running:
            try:
                # A DEALER socket receives messages without the server's identity frame
                recv_message = await self.socket.recv()
                
                # The payload contains our custom-packed data with the request_id
                request_id, received_data = unpack(recv_message[:-8]) # Assuming checksum is appended

                if request_id in self._pending_requests:
                    await self._pending_requests[request_id].put(received_data)
                else:
                    print(f"[ZMQ Warning] Received message for unknown request_id: {request_id}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[ZMQ Recv Loop Error] {e}")
                await asyncio.sleep(0.01)
        print("[ZMQ] Receiver loop stopped.")

    async def get_data(self, message: Dict[str, Any]):
        """
        Sends a request via the ZMQ DEALER socket and asynchronously yields responses
        from a dedicated queue populated by the central `_receive_loop`.
        """
        # request_id = str(uuid.uuid4())
        request_id=message["request_id"]
        response_queue = asyncio.Queue()
        self._pending_requests[request_id] = response_queue

        try:
            # serialize_msg = pack(message["topic"], message["body"], request_id=request_id)
            serialize_msg = pack(message)
            # print("zmq send ", serialize_msg)
            
            # Send the message asynchronously. ZMQ handles the non-blocking I/O.
            await self.socket.send(serialize_msg)

            while True:
                try:
                    # Wait for a response to appear in this request's specific queue
                    data = await asyncio.wait_for(response_queue.get(), timeout=10.0)
                    # print("receive data", data)

                    # Check for the server's shutdown/completion signal
                    if data.get("body") == b"shutdown":
                        yield "eof"
                        break
                    yield data

                except asyncio.TimeoutError:
                    print(f"[ZMQ Timeout] No response for request {request_id} within timeout period.")
                    yield "eof"
                    break
                except asyncio.CancelledError:
                    raise
        
        except Exception as e:
            print(f"[ZMQ Send Error] {e} for request {request_id}")
            yield "eof"
            
        finally:
            # Crucial: Clean up the pending request to prevent memory leaks
            self._pending_requests.pop(request_id, None)

    def close(self):
        """Gracefully shuts down the client."""
        if not self._running:
            return
        
        print("[ZMQ] Shutting down client...")
        self._running = False

        def shutdown_async_resources():
            if hasattr(self, '_receive_task') and self._receive_task:
                self._receive_task.cancel()
            if hasattr(self, 'socket'):
                self.socket.close()
            if hasattr(self, 'context'):
                self.context.term()

        # Schedule the cleanup on the event loop
        self.loop.call_soon_threadsafe(shutdown_async_resources)
        
        # Stop the event loop itself
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._loop_thread.join(timeout=2)
        self._executor.shutdown(wait=True)
        print("[ZMQ] Client shutdown complete.")
