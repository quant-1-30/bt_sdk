# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

import os 
os.environ['GRPC_ENABLE_FORK_SUPPORT']='0' # spawn  

import numpy as np
import pyarrow as pa
import grpc
import pyarrow.compute as pc

from typing import Iterator, AsyncIterator
from google.protobuf import empty_pb2
from google.protobuf.json_format import MessageToDict
from .serialize import service_pb2_grpc, service_pb2
from libc.stdint cimport int32_t

cdef int32_t MaxDate=30000000


cdef object Scale = {
        # benchmark amount not  1e-3
        RpcTopic.Tick: {
            "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1e-3, "amount": 1e-3,
        }, 
        RpcTopic.Close: {
            "tick": 1.0, "open": 1e-5, "high": 1e-5, "low": 1e-5, "close": 1e-5, "volume": 1e-3, "amount": 1e-3,
        },
        RpcTopic.Adjustment: {
            "bonus_share": 1e-3, "transfer": 1e-3, "bonus": 1e-3, # adjustment
        },
        RpcTopic.Rightment: {
            "price": 1e-3, "ratio": 1e-3 # rightment
        },
}

cdef inline object rpc_callback(bytes arrow_bytes, int32_t rpc_type): # inline function embed to reduce overhead when hundrends
    if not arrow_bytes:
        return None

    cdef object table = pa.ipc.open_stream(pa.py_buffer(arrow_bytes)).read_all() # open_stream zero_copy parse and pc zero_copy and vectorize 
    cdef int n = table.num_columns
    cdef list names = table.schema.names
    cdef list new_cols = [None] * n
    cdef dict scale = Scale.get(rpc_type, {}) # calendar and instrument

    cdef object col
    cdef object factor
    cdef object name

    for i in range(n):
        name = names[i]
        col  = table.column(i)
        # ---------- cast ----------
        if name == "sid" or name == "name":
            # if pa.types.is_binary(col.type):
            col = pc.cast(col, pa.string()) # better than table.set_column 
        # ---------- scale ----------
        elif name in scale:
            factor = scale[name]
            col = pc.round(pc.multiply(col, factor), ndigits=2)
        new_cols[i] = col

    table = pa.Table.from_arrays(
        new_cols,
        names=names
    ).replace_schema_metadata(table.schema.metadata)
    return table


cdef class RpcClient:

    def __init__(self, str host="localhost", int port=50051):
        self.host = host
        self.port = port

    async def __aenter__(self):
        await self.initialize()
        return self

    async def initialize(self, MAX_MESSAGE_LENGTH=512 * 1024 * 100):
        """
         grpc.keepalive_time_ms: The period (in milliseconds) after which a keepalive ping is
             sent on the transport.
         grpc.keepalive_timeout_ms: The amount of time (in milliseconds) the sender of the keepalive
             ping waits for an acknowledgement. If it does not receive an acknowledgment within this
             time, it will close the connection.
         grpc.keepalive_permit_without_calls: If set to 1 (0 : false; 1 : true), allows keepalive
             pings to be sent even if there are no calls in flight.
         grpc.http2.max_pings_without_data: How many pings can the client send before needing to
             send a data/header frame.
         For more details, check: https://github.com/grpc/grpc/blob/master/doc/keepalive.md
         """
        if self._channel is not None:
            return
            
        channel_options = [
            # same with server
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),

            # stream control http2 tcp ack
            ("grpc.http2.initial_window_size", 32 * 1024 * 1024),
            ("grpc.http2.initial_connection_window_size", 64 * 1024 * 1024),

            ("grpc.keepalive_time_ms", 30000),             # 30s Ping > Server 10s
            ("grpc.keepalive_timeout_ms", 10000),          # wait 10s
            ("grpc.keepalive_permit_without_calls", 1),    # 
            ("grpc.http2.max_pings_without_data", 0),      #
        ]
        
        self._channel = grpc.aio.insecure_channel(
            f"{self.host}:{self.port}",
            compression=None, # parrow+lz4 avoid grpc.Compression.Gzip  
            options=channel_options
        )
        self._stub = service_pb2_grpc.btDataFeedStub(self._channel)

    async def ensure_initialized(self):
        if self._channel is None:
            await self.initialize()
    
    cdef object _instrumentCall(self, object stub_req, bint wait_for_ready=True):
        response_iterator = self._stub.InstrumentCall(stub_req, wait_for_ready=wait_for_ready)
        return response_iterator

    cdef object _tickCall(self, object stub_req, bint wait_for_ready=True):
        response_iterator = self._stub.TickStreamCall(stub_req, wait_for_ready=wait_for_ready)
        return response_iterator

    cdef object _closeCall(self, object stub_req, bint wait_for_ready=True):
        response_iterator = self._stub.CloseStreamCall(stub_req, wait_for_ready=wait_for_ready)
        return response_iterator

    cdef object _adjustmentCall(self, object stub_req, bint wait_for_ready=True):
        response_iterator = self._stub.AdjustmentStreamCall(stub_req, wait_for_ready=wait_for_ready)
        return response_iterator

    cdef object _rightmentCall(self, object stub_req, bint wait_for_ready=True):
        response_iterator = self._stub.RightStreamCall(stub_req, wait_for_ready=wait_for_ready)
        return response_iterator

    cdef object _dispatch_rpc(self, int rpc_type, object req_body):
        cdef object request
        cdef object response_iterator

        if req_body:
            request = service_pb2.QuoteRequest(start_date=req_body.start_date, end_date=req_body.end_date, sid=req_body.sid)
        else:
            request = service_pb2.QuoteRequest(end_date=MaxDate)
            
        if rpc_type == RpcTopic.Instrument:
            response_iterator = self._instrumentCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Tick:
            response_iterator = self._tickCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Close:
            response_iterator = self._closeCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Adjustment:
            response_iterator = self._adjustmentCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Rightment:
            response_iterator = self._rightmentCall(request, wait_for_ready=True)
        else:
            raise ValueError(f"Unknown RPC type: {rpc_type}")
        return response_iterator

    async def on_request(self, int32_t rpc_type, object req_body):
        cdef object response, callback
        cdef object resp

        await self.ensure_initialized()
        response = self._dispatch_rpc(rpc_type, req_body)

        async for resp in response:
            yield rpc_callback(resp.payload, rpc_type)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        return False # True means suppress
    
    async def cleanup(self):
        await self._channel.close()
