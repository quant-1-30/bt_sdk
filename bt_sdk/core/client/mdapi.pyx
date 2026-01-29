# cython: language_level=3

import asyncio
import msgspec
import reactivex.operators as ops
import pyarrow as pa
# from contextlib import contextmanager

from bt_sdk.core.protocol import Event
from bt_sdk.core.client.async_client cimport AsyncZmqClient 
from bt_sdk.core.client.util cimport fast_uuid4_bytes


async def _collect_async(observable, loop, timeout):

    cdef list buffer = []
    cdef object fut = loop.create_future()
    cdef object subscription

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


cdef inline object _merge_tables(list result):
    if result and len(result) > 0:
        try:
            # return pa.Table.from_batches(result)  # pa.RecordBatch
            return pa.concat_tables(result, promote_options="permissive") # zero_copy accumlate chunk ptr not reallocate / just when combine_chunks() 
        except Exception as e:
            print(f"[MdApi] Merge error: {e}")
            return None
    return None


cdef class MdApi:

    def __init__(self,
                    tuple addr=("127.0.0.1", 8888),
                    int timeout = 30):
        self.async_client = AsyncZmqClient(addr=addr, timeout=timeout)

        try:
            self.loop = asyncio.get_running_loop() # Ray Async Actor
        except RuntimeError:
            self.loop = asyncio.new_event_loop() # Local
            asyncio.set_event_loop(self.loop)

    def __enter__(self):
        return self 

    async def async_def_calendar(self): 
        """
            request calendar
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Calendar)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.loop, self.timeout)
        data = _merge_tables(tables)
        return data

    cpdef object get_calendar(self):
        coroutine = self.async_def_calendar()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()
    
    async def async_get_instrument(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Instrument)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.loop, self.timeout)
        data = _merge_tables(tables)
        return data

    cpdef object get_instrument(self):
        coroutine = self.async_get_instrument()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()
    
    async def async_get_benchmark(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Index, body=body)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.loop, self.timeout)
        data = _merge_tables(tables)
        return data
    
    cpdef object get_benchmark(self):
        coroutine = self.async_get_benchmark()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()

    cpdef object subscribe(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Tick, body=body)

        obs = self.async_client.run(req_id, event)
        return obs
        
    cpdef object _close_obs(self, object body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Close, body=body)
        
        obs = self.async_client.run(req_id, event)
        return obs

    async def get_close(self, object body):
        obs = self._close_obs(body)
        tables = await _collect_async(obs, self.loop, self.timeout)
        data = _merge_tables(tables)
        return data

    cpdef object _event_obs(self, int topic, object body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)
        
        obs = self.async_client.run(req_id, event)
        return obs

    async def get_event(self, int topic, object body):
        obs = self._event_obs(topic, body)
        tables = await _collect_async(obs, self.loop, self.timeout)
        data = _merge_tables(tables)
        return data
    
    async def get_factor(self, object body):
        obs_close = self._close_obs(body)
        obs_adjust = self._event_obs(RpcTopic.Adjustment, body)
        obs_right = self._event_obs(RpcTopic.Rightment, body)
        
        coro1 = _collect_async(obs_close, self.loop, self.timeout)
        coro2 = _collect_async(obs_adjust, self.loop, self.timeout)
        coro3 = _collect_async(obs_right, self.loop, self.timeout)
        res_close, res_adjust, res_right = await asyncio.gather(coro1, coro2, coro3)
        
        close = _merge_tables(res_close)
        adjust = _merge_tables(res_adjust)
        right = _merge_tables(res_right)
        
        from bt_sdk.core.helper.factor import calc_factor
        factors = calc_factor(close, adjust, right)
        return factors

    cpdef void disconnect(self):
        self.async_client.close()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        self.async_client.close()


__all__ = ["MdApi"]
