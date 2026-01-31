# cython: language_level=3

import asyncio
import msgspec
import reactivex.operators as ops
import pyarrow as pa
import threading
# from contextlib import contextmanager

from bt_sdk.core.protocol import Event
from bt_sdk.core.client.async_client cimport AsyncZmqClient 
from bt_sdk.core.client.util cimport fast_uuid4_bytes
from bt_sdk.core.helper.factor import calc_factor


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
    # .pipe( # pipe return observal
    #         ops.buffer_with_time_or_count(
    #             timespan=0.5,            
    #             count=self.p.batch_size    
    #             ),
    # # ops.do_action(on_next=process_batch)
    # )

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
        async_client = AsyncZmqClient(addr=addr, timeout=timeout)

        self.async_client = async_client
        self.timeout = timeout
        
        self._init_event_loop() # used for sync
    
    def __enter__(self):
        return self 

    cdef void _init_event_loop(self):
        try:
            self.loop = asyncio.get_running_loop() # Ray Actor Loop 
            print(f"[{self.__class__.__name__}] Attached to existing Event Loop: {id(self.loop)}")
            is_background = False
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            if hasattr(self.loop, 'set_debug'):
                self.loop.set_debug(False)
        
            self._loop_thread = threading.Thread(
                target=self._run_event_loop,
                daemon=True,
                name="AsyncClient-EventLoop"
            )
            self._loop_thread.start()
            print(f"[{self.__class__.__name__}] Started internal background thread.")
            is_background = True
            
        self.async_client.attach_loop(self.loop, is_background=is_background)
        
    cdef void _run_event_loop(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        except Exception as e:
            print(f"Event loop error: {e}")

# --------------------------------------------------------------- Ray Async Actor --------------------------------------------------------
    
    async def get_calendar_async(self): 
        """
            request calendar
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Calendar)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_instrument_async(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Instrument)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_benchmark_async(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Index, body=body)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data
    
    async def get_close_async(self, object body):
        obs = self.get_close_obs(body)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_event_async(self, int topic, object body):
        obs = self.get_event_obs(topic, body)
        tables = await _collect_async(obs, self.timeout)
        data = _merge_tables(tables)
        return data

    async def get_factor_async(self, object body):
        obs_close = self.get_close_obs(body)
        obs_adjust = self.get_event_obs(RpcTopic.Adjustment, body)
        obs_right = self.get_event_obs(RpcTopic.Rightment, body)
        
        coro1 = _collect_async(obs_close, self.timeout)
        coro2 = _collect_async(obs_adjust, self.timeout)
        coro3 = _collect_async(obs_right, self.timeout)
        res_close, res_adjust, res_right = await asyncio.gather(coro1, coro2, coro3)
        
        close = _merge_tables(res_close)
        adjust = _merge_tables(res_adjust)
        right = _merge_tables(res_right)
        
        factors = calc_factor(close, adjust, right)
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
        
    cpdef object get_close_obs(self, object body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Close, body=body)
        
        obs = self.async_client.run(req_id, event)
        return obs

    cpdef object get_event_obs(self, int topic, object body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)
        
        obs = self.async_client.run(req_id, event)
        return obs

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
