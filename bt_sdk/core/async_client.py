#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import asyncio
import pickle
import zmq
import zmq.asyncio
import threading

from time import time
from functools import lru_cache
from typing import Dict, Any

from .exception import RemoteException
from .const import HEARTBEAT_TOPIC, HEARTBEAT_TOLERANCE


class AsyncStreamClient:

    # asyncio.open_connection() --- socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
    # getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)

    def __init__(self, addr):
        self.host, self.port = addr
        self.buffer = 1024

    async def get_data(self, req):
        reader, writer = await asyncio.open_connection(host=self.host, port=self.port)
        message = pickle.dumps(req.model_dump())
        print(f'Send: {message!r}')
        writer.write(message)
        await writer.drain()

        chunks=b""
        while True:
            # recv 面向连接 
            recv_message = await reader.read(self.buffer)
            print("recv_message", recv_message)
            chunks = chunks + recv_message 
            if recv_message == b"sentinel": 
                try:
                    received = pickle.loads(chunks)
                    yield received
                except Exception as e:
                    print("error", e)
                print("Received: {}".format(len(received)))
                chunks = b""
            elif recv_message == b"shutdown":
                print('Close the connection')
                writer.close()
                await writer.wait_closed()
                break

    async def on_receive(self, req): 
        result = []
        async for data in self.get_data(req):
            print("data", data)
            if data:
                result.append(data)
        return result

    def run(self, req):
        raw = asyncio.run(self.on_receive(req))
        return raw    

    # def on_exit(self):
    #     print("Closing socket")
    #     self.sock.close()


class AsyncDatagramClient(object):
        
        # transport sendto / abort
        # transport.sendto(message, self.addr)
        # sock = transport.get_extra_info("socket")
        # transport.close()
        # Resource temporarily unavailable need sleep or pdb

    def __init__(self, addr):
        self.buffer_size = 1024
        self.addr = addr
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)

    async def get_data(self, message):
        loop = asyncio.get_running_loop()
        
        print(f"Sending message to {self.addr}")
        # no attribute
        # loop.sock_sendto(self.sock, message, self.addr)
        self.sock.sendto(message, self.addr)
        
        chunks = b""
        try:
            while True:
                try:
                    # no attribute
                    # recv_message, _ = await loop.sock_recvfrom (self.sock, self.buffer)
                    recv_message = await loop.sock_recv (self.sock, self.buffer_size)
                    # print(f"Received chunk: {recv_message[:100]}...")  # Print first 100 bytes
                    
                    if not recv_message:
                        print("Connection closed by server")
                        break
                        
                    chunks += recv_message
                    
                    if recv_message == b"sentinel":
                        print("Sentinel received, processing chunks...")
                        try:
                            received = pickle.loads(chunks[:-8])  # Remove sentinel
                            # print(f"Successfully unpickled data: {received}")
                            yield received
                        except Exception as e:
                            print(f"Error unpickling data: {e}")
                            print(f"Chunks content: {chunks}")
                        chunks = b""
                    elif recv_message == b"shutdown":
                        print("Shutdown signal received")
                        break
                    
                except BlockingIOError:
                    print("Socket would block, waiting...")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error during reception: {e}")
                    break
                    
        except Exception as e:
            print(f"Outer loop error: {e}")
        finally:
            print("Exiting get_data")

    async def on_receive(self, req: Dict[str, Any]):
        # Get a reference to the event loop as we plan to use
        # low-level APIs.
        datas = []
        try:
            # message = pickle.dumps(req.model_dump())
            message = pickle.dumps(req)
            print(f"Serialized request: {message[:100]}...")  # Print first 100 bytes
            
            async for data in self.get_data(message):
                # print(f"Received data chunk: {data}")
                datas.append(data)
                
        except Exception as e:
            print(f"Error in on_receive: {e}")
        return datas
    
    def run(self, req: Dict[str, Any]):
        print(f"Starting run with request: {req}")
        try:
            resp = asyncio.run(self.on_receive(req))
            # print(f"Run completed with response: {resp}")
            print(f"Run completed with response: {len(resp)}")
            return resp
        except Exception as e:
            print(f"Error in run: {e}")
            raise

    def on_exit(self):
        print("Closing socket")
        self.sock.close()


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
