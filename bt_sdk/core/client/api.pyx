# cython: language_level=3

import asyncio
import msgspec
import reactivex.operators as ops
import pyarrow as pa
import threading

from bt_sdk.core.protocol import Event
from bt_sdk.core.client.async_client cimport AsyncRpcClient
from bt_sdk.core.client.util cimport fast_uuid4_bytes, _merge_tables, init_event_loop
from bt_sdk.core.factor import calc_factor


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

    def __init__(self,
                    tuple addr=("127.0.0.1", 8888),
                    int timeout = 30):
        self.async_client = AsyncRpcClient(addr=addr, timeout=timeout)
        self.timeout = timeout
        
        loop, is_background = init_event_loop() # used for sync
        self.async_client.attach_loop(loop, is_background=is_background)
        self.loop = loop
    
    def __enter__(self):
        return self 
        
# --------------------------------------------------------------- Ray Async Actor --------------------------------------------------------
    
    async def get_calendar_async(self): 
        """
            request calendar
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Calendar)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables, is_group=False)
        return data

    async def get_instrument_async(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Instrument)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables, is_group=False)
        return data

    async def get_benchmark_async(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Index, body=body)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data
    
    async def get_close_async(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Close, body=body)
        
        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_event_async(self, int topic, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)
        
        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_subscribe_async(self,object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(RpcTopic.Tick, body=body)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_factor_async(self, object body):
    
        cdef list[bytes]  sids = body.sid
        cdef object coro1 = self.get_close_async(body)
        cdef object coro2 = self.get_event_async(RpcTopic.Adjustment, body)
        cdef object coro3 = self.get_event_async(RpcTopic.Rightment, body)
        
        close, adj, rgt = await asyncio.gather(coro1, coro2, coro3)
        print("get_factor close ", close)
        print("get_factor adj ", adj)
        print("get_factor rgt ", rgt)
        factors = calc_factor(close, adj, rgt, sids)
        print("get_factor ", factors)
        return factors

# --------------------------------------------------------------- Sync and Local --------------------------------------------------------

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

    cpdef object subscribe(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Tick, body=body)

        obs = self.async_client.run(req_id, event)
        return obs

    cpdef object get_subscribe(self, object body):
        coroutine = self.get_subscribe_async(body)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result() 
        
    cpdef object get_close(self, object body):
        """
            request instruments
        """
        coroutine = self.get_close_async(body)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef object get_event(self, int topic, object body):
        """
            request instruments
        """
        coroutine = self.get_event_async(topic, body)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef object get_factor(self, object body):
        coroutine = self.get_factor_async(body)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef void disconnect(self):
        self.async_client.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        self.async_client.close()

