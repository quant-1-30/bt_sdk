import struct
import asyncio
import socket
import uuid

from bt_sdk.constant import CHUNK_HEADER_FORMAT
from bt_sdk.utils.serialize import pack, unpack
from bt_sdk.core.client.async_client import AsyncClient


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