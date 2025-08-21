#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import struct
import socket
import asyncio
import threading
from queue import Queue
from typing import Dict, Any, Set, Optional
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor
from bt_sdk.constant import CHUNK_HEADER_FORMAT

from bt_sdk.utils.serialize import pack, unpack


class AsyncClient:
    """
    高性能异步客户端，优化了单例模式、事件循环管理和任务调度
    """

    _instance = None
    _initialization_complete = False
    _lock_instance = threading.RLock()  # 使用RLock提升性能

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
        self.buffer_size = int(kwargs.get("buffer_size", 512))  # 默认缓冲区改为512B，适合流式处理
        
        self._tasks = set()
        self._message_queue = deque()
        self._queue_lock = threading.Lock()
        
        self._connection_pool = {}
        self._connection_stats = defaultdict(int)

        self._executor = ThreadPoolExecutor(
            max_workers=min(4, os.cpu_count() or 1),
            thread_name_prefix="AsyncClient"
        )
        
        self._init_event_loop()
    
    def _init_event_loop(self):
        """优化的事件循环初始化"""
        self.loop = asyncio.new_event_loop()

        # 定义一个普通的同步函数，用于创建需要绑定到循环的资源
        def init_loop_resources():
            # 确保 _async_reader_lock 被正确地绑定到 self.loop。
            self._async_reader_lock = asyncio.Lock()
            # 还有其他 asyncio 对象，也应该在这里创建

        future = self.loop.run_in_executor(None, init_loop_resources)
        self.loop.run_until_complete(future)

        if hasattr(self.loop, 'set_debug'):
            self.loop.set_debug(False)  # 生产环境关闭调试
        
        self._loop_thread = threading.Thread(
            target=self._run_event_loop,
            daemon=True,
            name="AsyncClient-EventLoop"
        )
        self._loop_thread.start()
        
        # 等待事件循环启动
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
       
        end_event = threading.Event()
        
        if not self._running:
            end_event.set()  # 立即设置结束事件
            self._ensure_eof(req_q, end_event) # 创建结束事件，避免依赖队列EOF
            return
        try:
            coro = self.on_receive(req, req_q, end_event)
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
            self._tasks.add(future)
            
            def cleanup_callback(f):
                try:
                    f.result()
                except Exception as e:
                    print(f"[CALLBACK ERROR] {e}")
                finally:
                    self._tasks.discard(f)
                    end_event.set()
                    self._ensure_eof(req_q, end_event)
            future.add_done_callback(cleanup_callback)
            
        except Exception as e:
            print(f"Error starting task: {e}")
            end_event.set()
            self._ensure_eof(req_q, end_event)
    
    async def on_receive(self, message: Dict[str, Any], req_q: Queue, end_event: threading.Event = None):
        """优化的消息接收处理 - 支持结束事件"""
        task = asyncio.current_task()
        try:
            data_count = 0
            async for data in self.get_data(message):
                if end_event and end_event.is_set(): 
                    break
                    
                req_q.put(data)
                data_count += 1
                if data == "eof":
                    break
                    
                if data_count % 100 == 0: # 批量处理优化：每100条消息让出控制权
                    await asyncio.sleep(0)
                    
        except asyncio.CancelledError:
            self._task_stats['cancelled'] += 1
            if end_event:
                end_event.set()
            raise
        except Exception as e:
            print(f"Error in on_receive: {e}")
            if end_event:
                end_event.set()
        finally: # 不管try or except 存在return / raise 都会执行
            if hasattr(self, '_tasks') and task:
                self._tasks.discard(task)
            # 确保结束事件被设置
            if end_event:
                end_event.set()
    
    def _ensure_eof(self, req_q, end_event):
        
        def _worker():
            """EOF投递工作线程 - 不放弃直到成功"""
            max_attempts = 3  # 减少最大尝试次数到3次
            attempt = 0
            base_delay = 0.001   # 减少起始延迟到1ms
            
            while attempt < max_attempts and not end_event.is_set():
                try:
                    if hasattr(req_q, 'put_nowait'):
                        req_q.put_nowait("eof")
                        return True
                except Exception:
                    pass
                
                delay = min(base_delay * (attempt + 1), 0.01) # 线性退避延迟，但不超过10ms
                time.sleep(delay)
                attempt += 1
            
            end_event.set() # 如果还是失败，强制设置结束事件        
            return False
        
        try:
            # self._executor.submit(eof_delivery_worker) # main thread finshed and submit will cause schedule new futures after interpreter shutdown
            loop = loop or asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError("Loop already closed")
            loop.call_soon_threadsafe(_worker) # 提交到 asyncio 主线程事件循环中
        except Exception as e:
            thread = threading.Thread(target=_worker, daemon=True) # 兜底方案：启用后台线程
            thread.start()

    def stop(self):
        if not self._running:
            return
            
        self._running = False
        
        for task in self._tasks: # 立即设置所有活跃任务的结束事件
            if not task.done():
                task.cancel()
        
        self._close_connection_pool()
        if hasattr(self, 'sock'):
            try:
                self.sock.close()
                print("Socket closed.")
            except Exception as e:
                print(f"Error closing socket: {e}")

        if hasattr(self, 'loop') and self.loop.is_running(): 
            cleanup_future = asyncio.run_coroutine_threadsafe(
                self._async_cleanup(), self.loop
            )
            try:
                cleanup_future.result(timeout=1.0) 
            except Exception as e:
                print(f"Error during async cleanup: {e}")

        if hasattr(self, '_executor'):
            try:
                self._executor.shutdown(wait=True) 
            except Exception as e:
                print(f"Error shutting down executor: {e}")     
        self._stop_event_loop()
    
    def _close_connection_pool(self):
        """关闭连接池"""
        for conn in self._connection_pool.values():
            try:
                if hasattr(conn, 'close'):
                    conn.close()
            except Exception as e:
                print(f"Error closing connection: {e}")
        self._connection_pool.clear()
    
    async def _async_cleanup(self):
        """异步清理资源"""
        try:
            await self._batch_cleanup_tasks()
            await self.loop.shutdown_asyncgens()
        except Exception as e:
            print(f"Error during async cleanup: {e}")
    
    async def _batch_cleanup_tasks(self):
        """批量清理任务"""
        if not self._tasks:
            return
            
        # 批量取消任务
        tasks_to_cancel = [t for t in self._tasks if not t.done()]
        if tasks_to_cancel:
            for task in tasks_to_cancel:
                task.cancel()
            # 等待任务完成，但有超时
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_cancel, return_exceptions=True),
                    timeout=3.0
                )
            except asyncio.TimeoutError:
                print("Warning: Some tasks did not complete within timeout")
    
    def _stop_event_loop(self):
        if hasattr(self, 'loop'):
            try:
                self.loop.call_soon_threadsafe(self.loop.stop)
                print("Event loop stop requested")
                # 等待循环线程结束 - 减少等待时间
                if hasattr(self, '_loop_thread') and self._loop_thread.is_alive():
                    self._loop_thread.join(timeout=1.0)  # 减少超时时间到1秒
            except Exception as e:
                print(f"Error stopping event loop: {e}")
    

class AsyncDatagramClient(AsyncClient):
    """优化的UDP客户端"""
    
    def __init__(self, addr):
        self.addr = addr
        self.HEADER_SIZE = struct.calcsize(CHUNK_HEADER_FORMAT)
        self._init_socket()
    
    def _init_socket(self):
        """优化的socket初始化"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)
        try:
            # 设置更小的socket缓冲区大小，提高响应速度
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.buffer_size)  # 发送缓冲区
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_size)  # 接收缓冲区
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # 启用地址重用
            
        except Exception as e:
            print(f"Socket optimization warning: {e}")

    async def get_data(self, message):
        """优化的UDP数据获取不拆包 --- 服务端row返回 应该不超过mtu 1472 """
        try:
            serialize_msg = pack(message["topic"], message["msg"])
            
            await self.loop.run_in_executor( # 使用线程池异步发送数据
                self._executor, 
                self.sock.sendto, 
                serialize_msg, 
                self.addr
            )
            while self._running:
                try:
                    async with self._async_reader_lock:
                        recv_message = await self.loop.sock_recv(self.sock, self.buffer_size)
                    if not recv_message:
                        yield "eof"
                        break

                    if recv_message[-8:] == b'sentinel':
                       try:
                           received = unpack(recv_message[:-8])
                           yield received
                       except Exception as e:
                           print(f"[Unpack Error] {e}")
                       finally:
                           continue  # 清空缓冲区，准备接收下一批数据
                        
                    if recv_message == b"shutdown":
                        print("Shutdown signal received")
                        yield "eof"
                        break
                except Exception as e:
                    print(f"[Recv Error] {e}")
                    break
                    
        except Exception as e:
            print(f"[UDP Error] {e}")
            yield "eof"
    

class AsyncStreamClient(AsyncClient):
    """优化的TCP客户端"""

    def __init__(self, addr):
        self.host, self.port = addr
        self._connection_cache = {}  # 连接复用缓存
        self.LENGTH_BYTES = 4
        self.timeout = 5.0

    async def recv_message(self,reader: asyncio.StreamReader):
        """
        支持大消息分块读取 + 自动解压 gzip + 自动 msgpack 解码
        无数据时立即返回，避免超时等待
        """
        chunks = bytearray()  # 使用bytearray提升内存效率
        batch_count = 0
        try:
            # 检查连接是否已关闭
            if reader.at_eof():
                print("[recv_message] connection is on eof")
                return None
            try:
                async with self._async_reader_lock:
                    length_bytes = await reader.readexactly(self.LENGTH_BYTES) # 尝试非阻塞地读取长度字节
            except asyncio.IncompleteReadError: # 无数据时立即返回
                # return None
                return "eof"
                
            msg_len = int.from_bytes(length_bytes, byteorder='big')
            if msg_len == 0:  # 如果长度为0，则返回EOF
                return "eof"
    
            while msg_len > 0:
                batch_count += 1
                reader_size = min(self.buffer_size, msg_len)
                try:
                    async with self._async_reader_lock:
                        chunk_bytes = await reader.readexactly(reader_size)
                except asyncio.IncompleteReadError:
                    # return  None 
                    return "eof"
                    
                chunks.extend(chunk_bytes) # append multiple bytes to a bytearray
                msg_len -= len(chunk_bytes)

                if batch_count % 100 == 0: # 定期让出控制权
                    await asyncio.sleep(0)

            data = bytes(chunks) # expand decompress
            return data
    
        except Exception as e:
            print(f"[recv_message error] {e}")
            return None

    async def get_data(self, message):
        """优化的TCP数据获取 采用reader.readexactly读取长度 然后根据长度读取数据 不需要sentinel或者shutdown标识"""
        connection_key = f"{self.host}:{self.port}"
        
        try:
            serialize_msg = pack(message["topic"], message["msg"], message["client_id"])
            reader, writer = await self._get_connection(connection_key) # 连接复用逻辑
            
            msg_len = len(serialize_msg)
            writer.write(msg_len.to_bytes(self.LENGTH_BYTES, byteorder='big'))
            writer.write(serialize_msg)
            await writer.drain()

            while True:
                recv_message = await self.recv_message(reader)
                if recv_message == "eof":
                    await self._close_connection(writer, connection_key)
                    break

                async for chunk in self._process_chunks(recv_message):
                    yield chunk
            # 数据处理完成后，关闭连接并返回EOF
            yield "eof"
            await self._close_connection(writer, connection_key)

        except Exception as e:
            print(f"[TCP Error] {e}")
            if connection_key in self._connection_cache:
                writer = self._connection_cache[connection_key][1]
                await self._close_connection(writer, connection_key)
            yield "eof"
    
    async def _process_chunks(self, chunks):
        """优化的数据块处理"""
        try:
            decoded = unpack(chunks)
            if decoded:
                yield decoded
        except Exception as e:
            print(f"[Process Chunks Error] {e}")
            yield "eof"
    
    async def _get_connection(self, connection_key):
        """获取或创建连接"""
        if connection_key in self._connection_cache:
            reader, writer = self._connection_cache[connection_key]
            if not writer.is_closing():
                self._connection_stats[connection_key] += 1
                return reader, writer
            else:
                # 清理失效连接
                del self._connection_cache[connection_key]
        
        # 创建新连接
        reader, writer = await asyncio.open_connection(
            host=self.host, 
            port=self.port
        )
        
        # 优化TCP连接参数
        sock = writer.get_extra_info('socket')
        if sock:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) # 禁用Nagle算法 减少小数据包的延迟
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except Exception as e:
                print(f"TCP optimization warning: {e}")
        
        self._connection_cache[connection_key] = (reader, writer)
        return reader, writer
    
    async def _close_connection(self, writer, connection_key):
        """关闭连接"""
        try:
            writer.close()
            await writer.wait_closed()
            if connection_key in self._connection_cache:
                del self._connection_cache[connection_key]
        except Exception as e:
            print(f"Error closing connection: {e}")
    