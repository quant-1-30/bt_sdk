#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import asyncio
import pickle
import threading
from queue import Queue
from typing import Dict, Any

from bt_sdk.utils.pack import msg_unpack


class AsyncClient:

    _instance = None
    _lock_instance = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock_instance:
                if not cls._instance:
                    cls._instance = super(AsyncClient, cls).__new__(cls)
                    cls._instance._running = True  # Flag to control the running state
                    cls._instance.buffer_size = int(kwargs.get("buffer_size", 1024))
                    cls._instance._tasks = set()  # Track active tasks
                    # Initialize the event loop
                    loop = asyncio.new_event_loop()
                    cls.activte_event_loop(loop)
                    cls._instance.loop = loop

        return cls._instance
    
    @classmethod
    def activte_event_loop(cls, loop):
        # if called in a context where no event loop is running
        def run_loop():
            asyncio.set_event_loop(loop)
            print("[loop] Event loop started")
            loop.run_forever()
        t = threading.Thread(target=run_loop, daemon=True)
        t.start()
    
    async def on_receive(self, message: Dict[str, Any], req_q: Queue):
        try:
            async for data in self.get_data(message):
                # print("on_receive ", data)
                req_q.put(data)
                if data == "eof":
                    # print("on_receive done")
                    break
        except Exception as e:
            print(f"Error in on_receive: {e}")
            req_q.put("eof")
        finally:
            # Ensure we clean up any resources
            if hasattr(self, '_tasks'):
                self._tasks.discard(asyncio.current_task())

    def run(self, req, req_q):
        coro = self.on_receive(req, req_q)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        # self.loop.run_in_executor(None, self.on_receive, req, tickerId) # cpu-bound task

        # Store the task for cleanup
        self._tasks.add(future)
        # Callback function to handle the result and cleanup
        def cleanup_callback(f):
            try:
                print("[CALLBACK TRIGGERED] ", f.result())
            except Exception as e:
                print(f"[CALLBACK ERROR] {e}")
            finally:
                self._tasks.discard(f)
        
        future.add_done_callback(cleanup_callback)
    
    def stop(self):
        """Stop the client and clean up all resources."""
        self._running = False
        
        # Cancel all pending tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
        
        # Close socket if it exists
        if hasattr(self, 'sock'):
            self.sock.close()
            print("client socket stopped.")
        
        # Clean up the event loop
        async def cleanup():
            # Cancel all tasks
            tasks = [t for t in asyncio.all_tasks(self.loop) if t is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            # Wait for all tasks to complete
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            # Shutdown async generators
            await self.loop.shutdown_asyncgens()
        
        # Run cleanup in the event loop
        if self.loop.is_running():
            asyncio.run_coroutine_threadsafe(cleanup(), self.loop)
        
        # Close the loop
        self.loop.call_soon_threadsafe(self.loop.stop)
        print("Closing event loop")


class AsyncDatagramClient(AsyncClient):
    
    # transport sendto / abort sendto(message, self.addr)
    # sock = transport.get_extra_info("socket")
        
    def __init__(self, addr):
        self.addr = addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)

    async def get_data(self, message):
        serialize_msg = pickle.dumps(message)
        # await self.loop.sock_sendto(self.sock, message, self.addr)
        await self.loop.run_in_executor(None, self.sock.sendto, serialize_msg, self.addr)

        chunks = b""
        while self._running:
            try:
                recv_message = await self.loop.sock_recv(self.sock, self.buffer_size)

                if not recv_message:
                    print("recv_message is empty", recv_message)
                    yield "eof"
                    break

                chunks += recv_message
                if chunks[-8:] == b"sentinel":
                    # print("Sentinel received, processing chunks...")
                    try:
                        # received = pickle.loads(chunks[:-8])
                        received = msg_unpack('md', message["topic"], chunks[:-8])
                        yield received
                    except Exception as e:
                        print(f"[Unpickle Error] {e}")
                        print(f"Chunks content: {chunks}")
                    chunks = b""
                elif chunks[-8:] == b"shutdown":
                    print("Shutdown signal received")
                    yield "eof"
                    break

            except Exception as e:
                print(f"[Recv Error] {e}")
                break


class AsyncStreamClient(AsyncClient):

    # asyncio.open_connection() --- socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    # getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)

    def __init__(self, addr):
        self.host, self.port = addr

    async def get_data(self, message):

        topic = message["topic"].split("_")[-1]

        serialize_msg = pickle.dumps(message)
        reader, writer = await asyncio.open_connection(host=self.host, port=self.port)
        writer.write(serialize_msg)
        print("writer.write", serialize_msg)
        await writer.drain()
        print("writer.drain")

        chunks = b""
        stats = 0
        while self._running:
            try:
                recv_message = await reader.read(self.buffer_size)
                # recv_message = await reader.read(100)
                import pdb
                print("recv_message ", len(recv_message), recv_message)
                stats += len(recv_message)
                print("stats ", stats)

                if not recv_message:
                    print("recv_message is empty", recv_message)
                    yield "eof"
                    await writer.wait_closed()
                    break

                chunks = chunks + recv_message 
                if recv_message[-8:] == b"sentinel":
                    # split chunkes by sentinel
                    splits = chunks.split(b'sentinel')
                    for chunk in splits:
                        decoded = msg_unpack('td', topic, chunk)
                        print("decoded", decoded)
                        yield decoded
                    chunks = b""

                elif recv_message[-8:] == b"shutdown":
                    print("Shutdown signal received")
                    yield "eof"
                    break

            except Exception as e:
                print(f"[Recv Error] {e}")
                break
