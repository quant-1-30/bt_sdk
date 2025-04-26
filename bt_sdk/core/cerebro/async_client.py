#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import asyncio
import pickle
import zmq
import zmq.asyncio
import threading
import collections
import itertools
from queue import Queue
from time import time
from functools import lru_cache
from typing import Dict, Any

from utils.packer import unpack
from core.exception import RemoteException


HEARTBEAT_TOPIC = "heartbeat"
HEARTBEAT_INTERVAL = 10
HEARTBEAT_TOLERANCE = 30


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
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.loop.close()
        print("Closing event loop")
    
    # def on_exit(self):
    #     """Clean up resources and close the loop."""
    #     print("Closing event loop")
    #     self.loop.run_until_complete(self.loop.shutdown_asyncgens())
    #     self.loop.close()


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
                # print("recv_message", recv_message)

                chunks += recv_message
                if recv_message == b"sentinel":
                    print("Sentinel received, processing chunks...")
                    try:
                        received = pickle.loads(chunks[:-8])
                        yield received
                    except Exception as e:
                        print(f"[Unpickle Error] {e}")
                        print(f"Chunks content: {chunks}")
                    chunks = b""
                elif recv_message == b"shutdown":
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

        topic = message["msg"].get("sub_topic", message["topic"])

        serialize_msg = pickle.dumps(message)
        reader, writer = await asyncio.open_connection(host=self.host, port=self.port)
        writer.write(serialize_msg)
        print("writer.write", serialize_msg)
        await writer.drain()
        print("writer.drain")

        chunks = b""
        while self._running:
            try:
                recv_message = await reader.read(self.buffer_size)
                print("recv_message", recv_message)

                if not recv_message:
                    print("recv_message is empty", recv_message)
                    yield "eof"
                    await writer.wait_closed()
                    break

                chunks = chunks + recv_message 
                if recv_message[-8:] == b"sentinel": 
                    decoded = unpack(topic, chunks[:-8])
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


class ZmqClient:

    def __init__(self) -> None:
        """Constructor"""
        # zmq port related
        self._context: zmq.Context = zmq.Context()

        # Request socket (Request–reply pattern)
        self._socket_req: zmq.Socket = self._context.socket(zmq.REQ)

        # Subscribe socket (Publish–subscribe pattern)
        self._socket_sub: zmq.Socket = self._context.socket(zmq.SUB)

        # Set socket option to keepalive
        for socket in [self._socket_req, self._socket_sub]:
            socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
            socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 60)

        # Worker thread relate, used to process data pushed from server
        self._active: bool = False                 # RpcClient status
        self._thread: threading.Thread = None      # RpcClient thread
        self._lock: threading.Lock = threading.Lock()

        self._last_received_ping: time = time()

    @lru_cache(100)
    def __getattr__(self, name: str) -> Any:
        """
        Realize remote call function
        """
        # Perform remote call task
        def dorpc(*args, **kwargs):
            # Get timeout value from kwargs, default value is 30 seconds
            if "timeout" in kwargs:
                timeout = kwargs.pop("timeout")
            else:
                timeout = 30000

            # Generate request
            req: list = [name, args, kwargs]

            # Send request and wait for response
            with self._lock:
                self._socket_req.send_pyobj(req)

                # Timeout reached without any data
                n: int = self._socket_req.poll(timeout)
                if not n:
                    msg: str = f"Timeout of {timeout}ms reached for {req}"
                    raise RemoteException(msg)

                rep = self._socket_req.recv_pyobj()

            # Return response if successed; Trigger exception if failed
            if rep[0]:
                return rep[1]
            else:
                raise RemoteException(rep[1])

        return dorpc

    def start(
        self,
        req_address: str,
        sub_address: str
    ) -> None:
        """
        Start RpcClient
        """
        if self._active:
            return

        # Connect zmq port
        self._socket_req.connect(req_address)
        self._socket_sub.connect(sub_address)

        # Start RpcClient status
        self._active = True

        # Start RpcClient thread
        self._thread = threading.Thread(target=self.run)
        self._thread.start()

        self._last_received_ping = time()

    def stop(self) -> None:
        """
        Stop RpcClient
        """
        if not self._active:
            return

        # Stop RpcClient status
        self._active = False

    def join(self) -> None:
        # Wait for RpcClient thread to exit
        if self._thread and self._thread.is_alive():
            self._thread.join()
        self._thread = None

    def run(self) -> None:
        """
        Run RpcClient function
        """
        pull_tolerance: int = HEARTBEAT_TOLERANCE * 1000

        poller = zmq.Poller()
        poller.register(self._socket_sub, zmq.POLLIN)
        poller.register(self._socket_req, zmq.POLLOUT)

        while self._active:
            socks = dict(poller.poll(pull_tolerance))

            if self._socket_sub in socks and socks[self._socket_sub] == zmq.POLLIN:
                self.on_disconnected()
                continue

            if self._socket_req in socks and socks[self._socket_req] == zmq.POLLOUT:
                # Ready to send a request
                # Example: self._socket_req.send_pyobj(your_request_object)
                pass

            # Receive data from subscribe socket
            topic, data = self._socket_sub.recv_pyobj(flags=zmq.NOBLOCK)

            if topic == HEARTBEAT_TOPIC:
                self._last_received_ping = data
            else:
                # Process data by callable function
                self.callback(topic, data)

        # Close socket
        self._socket_req.close()
        self._socket_sub.close()

    def callback(self, topic: str, data: Any) -> None:
        """
        Callable function / accumulate data
        """
        raise NotImplementedError

    def subscribe_topic(self, topic: str) -> None:
        """
        Subscribe data
        """
        self._socket_sub.setsockopt_string(zmq.SUBSCRIBE, topic)

    def on_disconnected(self):
        """
        Callback when heartbeat is lost.
        """
        msg: str = f"RpcServer has no response over {HEARTBEAT_TOLERANCE} seconds, please check you connection."
        print(msg)
