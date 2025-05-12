#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import asyncio
import pickle
import threading
import collections
import itertools
from queue import Queue
from typing import Dict, Any

from bt_sdk.utils.packer import td_unpack, md_unpack


class AsyncClient:

    _instance = None
    _lock_instance = threading.Lock()
    # transport sendto / abort
    # transport.sendto(message, self.addr)
    # sock = transport.get_extra_info("socket")
    # transport.close()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock_instance:
                if not cls._instance:
                    cls._instance = super(AsyncClient, cls).__new__(cls)
                    cls._instance.qs = collections.OrderedDict()  # key: tickerId -> queues
                    # cls._instance.ts = collections.OrderedDict()  # key: queue -> t
                    cls._instance._lock_q = threading.Lock()
                    cls._instance._tickerId = itertools.count()
                    cls._instance._running = True  # Flag to control the running state
                    cls._instance.buffer_size = 1024

                    # # Initialize the event loop
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
    
    def reuseQueue(self, tickerId):
        '''Reuses queue for tickerId, returning the new tickerId and q'''
        with self._lock_q:
            # Invalidate tickerId in qs (where it is a key)
            q = self.qs.pop(tickerId, None)  # invalidate old
            iscash = self.iscash.pop(tickerId, None)

            # Update ts: q -> ticker
            tickerId = self.nextTickerId()  # get new tickerId
            self.ts[q] = tickerId  # Update ts: q -> tickerId
            self.qs[tickerId] = q  # Update qs: tickerId -> q
            self.iscash[tickerId] = iscash

        return tickerId, q

    def getTickQueue(self, start=False):
        '''Creates tick/Queue for data delivery to a data feed'''
        q = Queue()
        if start:
            q.put(None)
            return q

        with self._lock_q:
            tickerId = next(self._tickerId)
            self.qs[tickerId] = q

        return tickerId
    
    async def on_receive(self, message: Dict[str, Any], tickerId: int):
        try:
            async for data in self.get_data(message):
                print("on_receive ", data)
                self.qs[tickerId].put(data)
                if data == "eof":
                    print("on_receive done")
                    break
        except Exception as e:
            print(f"Error in on_receive: {e}")
            self.qs[tickerId].put("eof")

    def run(self, req):
        tickerId = self.getTickQueue()
        print("run ", tickerId)
        # asyncio.create_task(self.on_receive(req, tickerId)) # asyncio.run
        # self.loop.run_in_executor(None, self.on_receive, req, tickerId) # cpu-bound task
        coro = self.on_receive(req, tickerId)
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        # Callback function to handle the result
        future.add_done_callback(lambda f: print("[CALLBACK TRIGGERED] ", f.result()))
        return self.qs[tickerId]
    
    def stop(self):
        """Stop the UDP / TCP client by setting the running flag to False and closing the socket."""
        self._running = False
        self.sock.close()
        print("client socket stopped.")
        """Clean up resources and close the loop."""
        # exit
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()
        print("Closing event loop")
    

class AsyncDatagramClient(AsyncClient):
        
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
                    print("Sentinel received, processing chunks...")
                    try:
                        # received = pickle.loads(chunks[:-8])
                        received = md_unpack(message["topic"], chunks[:-8])
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
                        decoded = td_unpack(topic, chunk)
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
