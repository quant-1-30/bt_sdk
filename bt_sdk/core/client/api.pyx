# cython: language_level=3

import asyncio
import msgspec
import reactivex.operators as ops
import pyarrow as pa
import threading
import polars as pl


from bt_sdk.core.protocol import Event
from bt_sdk.core.client.async_client cimport AsyncRpcClient
from bt_sdk.core.client.util cimport fast_uuid4_bytes, _merge_tables
from bt_sdk.core.factor import calc_factor, apply_factor

from libc.stdint cimport int32_t

cdef dict _md_api_registry = {} 
cdef object _md_api_lock = threading.Lock()

async def _collect_async(observable, timeout):
    cdef list buffer = []
    cdef object fut 
    cdef object subscription

    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def on_next(item):
        buffer.append(item)

    def on_completed():
        if not fut.done():
            loop.call_soon_threadsafe(fut.set_result, "DONE")

    def on_error(err):
        if not fut.done():
            loop.call_soon_threadsafe(fut.set_exception, err)

    subscription = observable.subscribe(
        on_next=on_next,
        on_error=on_error,
        on_completed=on_completed
    )
    try:
        await asyncio.wait_for(fut, timeout=timeout)
        return buffer
    except asyncio.TimeoutError:
        print(f"[MdApi] Timeout collecting data.")
        return []
    except Exception as e:
        print(f"[MdApi] Error collecting data: {e}")
        raise e
    finally:
        subscription.dispose()


cdef class MdApi:

    def __init__(self, tuple addr, int32_t timeout):
        self.async_client = AsyncRpcClient(addr=addr, timeout=timeout)
        self.timeout = timeout
        self.loop = None
        self._is_initialized = False
 
    cpdef start(self, object loop):
        if self._is_initialized:
            return
        self.loop = loop
        print(f"[MdApi] Attaching to Loop: {id(self.loop)}")
        self.async_client.attach_loop(self.loop, is_background=False) # reuse main loop avoid cross thread
        self._is_initialized = True
 
    def __enter__(self):
        return self
        
# --------------------------------------------------------------- Async Api --------------------------------------------------------
    
    async def get_calendar_async(self): 
        """
            request calendar
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Calendar)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables, is_group=False)
        return pl.from_arrow(data)

    async def get_instrument_async(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Instrument)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables, is_group=False)
        return pl.from_arrow(data)

    async def get_benchmark_async(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Index, body=body)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        raw_data = _merge_tables(tables)
        pl_data = {key: pl.from_arrow(val) for key, val in raw_data.items()}
        return pl_data

    async def get_raw_close_async(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Close, body=body)
        
        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        raw_data = _merge_tables(tables)
        return raw_data 

    async def get_close_async(self, object body, int32_t forward_type):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Close, body=body)
        
        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        raw_data = _merge_tables(tables)

        if forward_type == 0:
            pl_data = {key: pl.from_arrow(val) for key, val in raw_data.items()}
            return pl_data

        factors = await self.get_factor_async(body, forward_type)
        adjusted_array = apply_factor(raw_data, factors, forward_type)
        return adjusted_array 

    async def get_event_async(self, int topic, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)
        
        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        raw_data = _merge_tables(tables)
        pl_data = {key: pl.from_arrow(val) for key, val in raw_data.items()}
        return pl_data

    async def get_subscribe_async(self, object body, int32_t forward_type):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(RpcTopic.Tick, body=body)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        raw_data = _merge_tables(tables)

        if forward_type == 0:
            pl_data = {key: pl.from_arrow(val) for key, val in raw_data.items()}
            return pl_data

        factors = await self.get_factor_async(body, forward_type)
        adjusted_array = apply_factor(raw_data, factors, forward_type)
        return adjusted_array 

    async def get_factor_async(self, object body, int32_t forward):
    
        cdef object coro1 = self.get_raw_close_async(body) # raw
        cdef object coro2 = self.get_event_async(RpcTopic.Adjustment, body)
        cdef object coro3 = self.get_event_async(RpcTopic.Rightment, body)
        
        close, adj, rgt = await asyncio.gather(coro1, coro2, coro3)
        factors = calc_factor(close, adj, rgt, forward)
        return factors

# --------------------------------------------------------------- Sync Api --------------------------------------------------------

    cpdef object get_calendar(self):
        coroutine = self.get_calendar_async()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()
    
    cpdef object get_instrument(self):
        coroutine = self.get_instrument_async()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()
    
    cpdef object get_benchmark(self, object body):
        coroutine = self.get_benchmark_async(body)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef object get_subscribe(self, object body, int32_t forward_type):
        coroutine = self.get_subscribe_async(body, forward_type)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        adjusted_array = future.result()
        return adjusted_array 
        
    cpdef object get_close(self, object body, int32_t forward):
        """
            request instruments
        """
        coroutine = self.get_close_async(body, forward)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef object get_event(self, int topic, object body):
        """
            request instruments
        """
        coroutine = self.get_event_async(topic, body)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef object get_factor(self, object body, int32_t forward):
        coroutine = self.get_factor_async(body, forward)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef void disconnect(self):
        self.async_client.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        self.async_client.close()


cpdef MdApi GetMdApi(tuple addr, int32_t timeout=30):
    global _md_api_registry
    
    cdef MdApi instance

    with _md_api_lock:
        if addr in _md_api_registry:
            return _md_api_registry[addr]
            
        print(f"[MdApi] Initializing new instance for target: {addr}")
        instance = MdApi(addr=addr, timeout=timeout)
        _md_api_registry[addr] = instance
            
    return _md_api_registry[addr]
