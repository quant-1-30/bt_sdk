import asyncio
import msgpack
from typing import Optional, Dict, Any


class TCPClient:
    def __init__(self, host: str = 'localhost', port: int = 8888):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._recv_buf = bytearray(1024 * 1024)  # 1MB buffer
        self.LENGTH_BYTES = 8  # 使用8字节表示长度

    async def connect(self) -> None:
        """Connect to the server."""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        print(f"Connected to server at {self.host}:{self.port}")

    async def send_message(self, message: Dict[str, Any]) -> None:
        """Send a message with length prefix."""
        if not self.writer:
            raise RuntimeError("Not connected to server")

        # Pack the message using msgpack
        packed_msg = msgpack.packb(message)
        
        # Get the length of the packed message
        msg_len = len(packed_msg)
        
        # Send the length prefix (8 bytes, big-endian)
        self.writer.write(msg_len.to_bytes(self.LENGTH_BYTES, byteorder='big'))
        
        # Send the actual message
        self.writer.write(packed_msg)
        await self.writer.drain()

    async def receive_message(self) -> Optional[Dict[str, Any]]:
        """Receive a message with length prefix."""
        if not self.reader:
            raise RuntimeError("Not connected to server")

        try:
            # Read the length prefix (8 bytes)
            length_data = await self.reader.readexactly(self.LENGTH_BYTES)
            msg_len = int.from_bytes(length_data, byteorder='big')

            if msg_len > len(self._recv_buf):
                # 如果消息太大，动态调整缓冲区大小
                if msg_len > 1024 * 1024 * 1024:  # 如果超过1GB，可能需要分块处理
                    raise ValueError(f"Message too large: {msg_len} bytes (max 1GB supported)")
                self._recv_buf = bytearray(msg_len)

            # Read the actual message
            view = memoryview(self._recv_buf)[:msg_len]
            view[:] = await self.reader.readexactly(msg_len)
            
            # Unpack the message
            return msgpack.unpackb(view, raw=False)
        except asyncio.IncompleteReadError:
            print("Server disconnected")
            return None
        except Exception as e:
            print(f"Error receiving message: {str(e)}")
            return None

    async def close(self) -> None:
        """Close the connection."""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.reader = None
            self.writer = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

# Example usage:
async def example_client_usage():
    async with TCPClient() as client:
        # Send a message
        message = {
            "topic": "test_topic",
            "msg": "Hello, Server!",
            "client_id": "client1"
        }
        await client.send_message(message)

        # Receive response
        response = await client.receive_message()
        if response:
            print(f"Received response: {response}")
