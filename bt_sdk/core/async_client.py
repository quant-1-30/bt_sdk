#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import socket
import asyncio
import pickle
# import httpx
from urllib.parse import urlencode, urljoin
from typing import Dict, Any


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

    def on_exit(self):
        print("Closing socket")
        self.sock.close()


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


# class AsyncApiClient:

#     def __init__(self, addr):
#         self.addr = addr
#         self.client = httpx.AsyncClient()

#     async def get_data(self, req_map: Dict[str, Any]):
#         endpoint = req_map.pop("endpoint", '')
#         params = req_map.pop("params", {})
#         method = req_map.pop("method", "GET")
#         headers = req_map.pop("headers", {})
#         async with httpx.AsyncClient() as client:
#             url = urljoin(self.addr, endpoint)
#             if method == "GET":
#                 resp = await client.get(url, params=params, headers=headers)
#             else:
#                 resp = await client.post(url, json=params, headers=headers)
#         return resp.json()
    
#     async def get_stream(self, req_map: Dict[str, Any]):
#         endpoint = req_map.pop("endpoint", '')
#         params = req_map.pop("params", {})
#         method = req_map.pop("method", "GET")
#         url = urljoin(self.addr, endpoint)
#         async with httpx.AsyncClient() as client:
#             async with client.stream(method, url, params=params) as response:
#                 # aiter_bytes / aiter_text / aiter_lines  
#                 async for chunk in response.aiter_bytes():
#                     yield chunk

#     async def on_receive(self, req_map: Dict[str, Any]):
#         stream = req_map.pop("stream", False)
#         if not stream:
#             resp = await self.get_data(req_map)
#             return resp
#         # stream
#         result = []
#         async for message in self.get_stream(req_map):
#             result.append(message)
#         return result

#     def run(self, req_map: Dict[str, Any]):
#         return asyncio.run(self.on_receive(req_map))
