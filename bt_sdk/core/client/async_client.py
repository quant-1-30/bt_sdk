#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import struct
import socket
import asyncio
import threading
import uuid
from queue import Queue
from typing import Dict, Any, Set, Optional
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from bt_sdk.constant import CHUNK_HEADER_FORMAT

# 假设的序列化函数，现在必须能处理 request_id
# 您需要根据您的实际情况调整 pack 和 unpack 函数
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

        # 在事件循环线程中创建需要绑定到循环的资源
        def init_loop_resources():
            # self._async_reader_lock 不再需要在多个协程间共享读取，因此可以移除
            pass
        
        future = self.loop.run_in_executor(None, init_loop_resources)
        self.loop.run_until_complete(future)

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

    def run(self, req, req_q):
       
        if not self._running:
            self._ensure_eof(req_q) # 创建结束事件，避免依赖队列EOF
            return
        try:
            coro = self.on_receive(req, req_q)
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            self._tasks.add(future)
            
            def cleanup_callback(f):
                try:
                    f.result()
                except Exception as e:
                    print(f"[CALLBACK ERROR] {e}")
                finally:
                    self._tasks.discard(f)
                    self._ensure_eof(req_q)

            future.add_done_callback(cleanup_callback)
            
        except Exception as e:
            print(f"Error starting task: {e}")
            self._ensure_eof(req_q)
    
    async def on_receive(self, message: Dict[str, Any], req_q: Queue):
        """优化的消息接收处理 - 支持结束事件"""
        task = asyncio.current_task()
        try:
            data_count = 0
            async for data in self.get_data(message):
                req_q.put(data)
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


class AsyncDatagramClient(AsyncClient):
    """
    优化的UDP客户端 - 重构以支持安全的并发请求
    """
    
    def __init__(self, addr):
        # 确保基类初始化只在第一次发生
        if not self._initialization_complete:
            super()._initialize()
        
        self.addr = addr
        self.HEADER_SIZE = struct.calcsize(CHUNK_HEADER_FORMAT)
        self._init_socket()
        
        # 这个任务在客户端实例化时启动，并一直运行
        if hasattr(self, 'loop') and self.loop.is_running():
            self._receive_task = asyncio.run_coroutine_threadsafe(self._receive_loop(), self.loop)
        else:
            # 如果事件循环尚未完全启动，则延迟创建
            self.loop.call_soon_threadsafe(lambda: asyncio.create_task(self._receive_loop()))

    def _init_socket(self):
        """优化的socket初始化"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.buffer_size)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except Exception as e:
            print(f"Socket optimization warning: {e}")

    async def _receive_loop(self):
        """
            后台接收循环持续监听socket request_id 分发到对应的请求队列
        """
        print("[UDP] Receiver loop started.")
        while self._running:
            try:
                recv_message, _ = await self.loop.sock_recvfrom(self.sock, self.buffer_size)
                if not recv_message:
                    continue

                request_id, received_data = unpack(recv_message[:-8])

                if request_id in self._pending_requests:
                    await self._pending_requests[request_id].put(received_data)
                else:
                    print(f"[UDP Warning] Received message for unknown request_id: {request_id}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[UDP Recv Loop Error] {e}")
                await asyncio.sleep(0.01) # 发生错误时短暂暂停避免CPU空转
        print("[UDP] Receiver loop stopped.")

    async def get_data(self, message):
        """
        现在此方法负责发送请求，并从专属队列中异步地获取响应。
        """
        request_id = str(uuid.uuid4())
        response_queue = asyncio.Queue()
        self._pending_requests[request_id] = response_queue
        try:
            serialize_msg = pack(message["topic"], message["body"], request_id=request_id)
        
            await self.loop.run_in_executor(
                self._executor, 
                self.sock.sendto, 
                serialize_msg, 
                self.addr
            )

            while True:
                try:
                    data = await asyncio.wait_for(response_queue.get(), timeout=10.0)

                    # if data == "sentinel" or data == "shutdown":
                    if data["body"] == b"shutdown":
                        yield "eof"
                        break

                    yield data

                except asyncio.TimeoutError:
                    print(f"[UDP Timeout] No response for request {request_id} within timeout period.")
                    yield "eof"
                    break
                except asyncio.CancelledError:
                    raise
        
        except Exception as e:
            print(f"[UDP Send Error] {e} for request {request_id}")
            yield "eof"
            
        finally:
            # *** 重要 ***: 清理请求，避免内存泄漏
            self._pending_requests.pop(request_id, None)


class AsyncStreamClient(AsyncClient):
    """
    优化的TCP客户端 - 重构以支持在单一连接上安全地进行请求复用
    """
    def __init__(self, addr):
        if not self._initialization_complete:
            super()._initialize()
            
        self.host, self.port = addr
        # reader, writer, reader_task
        self._connection_cache: Dict[str, tuple] = {} 
        self.LENGTH_BYTES = 4
        self.timeout = 5.0

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
                print("payload :", payload)

                if request_id in self._pending_requests:
                    await self._pending_requests[request_id].put(payload)
                else:
                    print(f"[TCP Warning] Received message for unknown request_id: {request_id}")

            except (asyncio.IncompleteReadError, ConnectionResetError):
                print(f"[TCP] Connection {connection_key} closed.")
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[TCP Reader Error] for {connection_key}: {e}")
                break
        
        print(f"[TCP] Reader for {connection_key} stopped.")
        # 连接断开后，通知所有还在等待此连接响应的请求
        # (这是一个简化处理，更复杂的系统可能需要重试逻辑)
        # 此处我们通过关闭连接来触发get_data中的异常处理
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
        request_id = str(uuid.uuid4())
        response_queue = asyncio.Queue()
        self._pending_requests[request_id] = response_queue
        connection_key = f"{self.host}:{self.port}"

        try:
            _, writer = await self._get_connection(connection_key) # cache
            
            # 假设 pack 函数现在接受 request_id
            serialize_msg = pack(**message, request_id=request_id)
            
            msg_len = len(serialize_msg)
            writer.write(msg_len.to_bytes(self.LENGTH_BYTES, byteorder='big'))
            writer.write(serialize_msg)
            await writer.drain()

            while True:
                try:
                    data = await asyncio.wait_for(response_queue.get(), timeout=10.0)
                    print("get_data :", data)
                    # import pdb; pdb.set_trace()
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
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Error closing connection: {e}")
