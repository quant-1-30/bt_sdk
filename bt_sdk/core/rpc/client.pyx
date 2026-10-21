# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

from libc.stdint cimport int32_t

import os 
os.environ['GRPC_ENABLE_FORK_SUPPORT']='0' # spawn  

import asyncio
import logging
import pyarrow as pa
import grpc
import pyarrow.compute as pc

logger = logging.getLogger(__name__)

from bt_protocol.serialize.pb import bt_protocol_service_pb2, bt_protocol_service_pb2_grpc
from bt_protocol.constant import RpcTopic
from bt_sdk.core.rpc.scale_utils cimport get_scale_ndigits
from bt_sdk.core.rpc.constants import Scale, BenchmarkScale, MaxDate 


cpdef object rpc_callback(bytes arrow_bytes, int32_t rpc_type):
    if not arrow_bytes:
        return None

    cdef object table = pa.ipc.open_stream(pa.py_buffer(arrow_bytes)).read_all()
    cdef list names = table.schema.names
    cdef int n = len(names)
    cdef dict meta = table.schema.metadata
    cdef dict scale = (
        BenchmarkScale.get(rpc_type)
        if (meta is not None and meta.get(b"asset_type") == b"benchmark")
        else Scale.get(rpc_type)
    )
    
    cdef list cols = []
    cdef object col
    cdef object factor_val
    cdef double factor
    cdef int ndigits
    cdef str name
    cdef int i

    for i in range(n):
        name = names[i]
        col = table.column(i)

        if name == "sid" or name == "name":
            cols.append(pc.cast(col, pa.string()))
            continue

        if scale is not None and name in scale:
            factor_val = scale[name]
            factor = <double>factor_val
            ndigits = get_scale_ndigits(factor)

            # ndigits == 0 ---> int64 (e.g. tick=1.0, or benchmark volume/amount=1.0)
            if ndigits > 0:
                col = pc.round(pc.multiply(col, factor), ndigits=ndigits)

        cols.append(col)

    return pa.Table.from_arrays(cols, names=names, metadata=meta)


cdef class RpcClient:

    def __init__(self, str host="localhost", int port=50051):
        self.host = host
        self.port = port
        self._channel = None
        self._stub = None
        self._init_lock = None  # defer to initialize() avoid bind wrong loop

    async def __aenter__(self):
        await self.initialize()
        return self

    async def initialize(self, MAX_MESSAGE_LENGTH=64 * 1024 * 1024):
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

        # Lazy init lock in coroutine context. Since asyncio is single-threaded per loop,
        # the check-then-set is safe: no yield point between None check and assignment.
        if self._init_lock is None:
            self._init_lock = asyncio.Lock()

        async with self._init_lock:
            # double-check after acquiring the lock
            if self._channel is not None:
                return

            channel_options = [
                # same with server
                ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
                ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),

                # stream control http2 tcp ack
                ("grpc.http2.initial_window_size", 64 * 1024 * 1024),
                ("grpc.http2.initial_connection_window_size", 512 * 1024 * 1024),

                ("grpc.keepalive_time_ms", 30000),             # 30s Ping > Server 10s
                ("grpc.keepalive_timeout_ms", 10000),          # wait 10s
                ("grpc.keepalive_permit_without_calls", 1),
                ("grpc.http2.max_pings_without_data", 0),
            ]

            self._channel = grpc.aio.insecure_channel(
                f"{self.host}:{self.port}",
                compression=None, # parrow+lz4 avoid grpc.Compression.Gzip
                options=channel_options
            )
            self._stub = bt_protocol_service_pb2_grpc.btDataFeedStub(self._channel)

    async def ensure_initialized(self):
        if self._channel is None:
            await self.initialize()

    cdef object _dispatch_rpc(self, int rpc_type, object req_body):
        cdef object request

        if req_body is not None:
            request = bt_protocol_service_pb2.QuoteRequest(
                start_date=req_body.start_date, end_date=req_body.end_date, sid=req_body.sid
            )
        elif rpc_type == RpcTopic.Instrument:
            request = bt_protocol_service_pb2.QuoteRequest(end_date=MaxDate)
        else:
            raise ValueError(
                f"QueryBody req_body is required for RPC topic {rpc_type}; "
                "only RpcTopic.Instrument allows req_body=None"
            )

        if rpc_type == RpcTopic.Instrument:
            return self._stub.InstrumentCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Tick:
            return self._stub.TickStreamCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Daily:
            return self._stub.DailyStreamCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Close:
            return self._stub.CloseStreamCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Adjustment:
            return self._stub.AdjustmentStreamCall(request, wait_for_ready=True)
        elif rpc_type == RpcTopic.Rightment:
            return self._stub.RightStreamCall(request, wait_for_ready=True)
        raise ValueError(f"Unknown RPC type: {rpc_type}")

    async def on_request(self, int32_t rpc_type, object req_body):
        cdef object response, resp

        await self.ensure_initialized()
        response = self._dispatch_rpc(rpc_type, req_body)

        async for resp in response:
            yield rpc_callback(resp.payload, rpc_type)

    async def cleanup(self):
        # Capture and null references before awaiting close so any concurrent
        # initialize() sees None and builds a fresh channel + loop-bound lock.
        channel = self._channel
        self._channel = None
        self._stub = None
        self._init_lock = None
        if channel is not None:
            await channel.close()

    cpdef void hard_reset(self):
        """Drop channel state synchronously when the bound loop is already dead."""
        self._channel = None
        self._stub = None
        self._init_lock = None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        await self.cleanup()
        return False # True means suppress
