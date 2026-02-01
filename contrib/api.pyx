# cython: language_level=3

import asyncio
import msgspec
import reactivex.operators as ops
import pyarrow as pa
import threading

from bt_sdk.core.protocol import Event
from bt_sdk.core.client.async_client cimport AsyncZmqClient, AsyncStreamClient, AsyncRpcClient
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
    # .pipe(
    #  ops.buffer_with_time_or_count(
    #      timespan=0.5,            
    #      count=self.p.batch_size    
    #  )
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


cdef class MdApi:

    def __init__(self,
                    tuple addr=("127.0.0.1", 8888),
                    int timeout = 30):
        self.async_client = AsyncZmqClient(addr=addr, timeout=timeout)
        self.timeout = timeout
        
        loop, is_background = init_event_loop() # used for sync
        self.async_client.attach_loop(loop, is_background=is_background)
        self.loop = loop
    
    def __enter__(self):
        return self 
        
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
        coro1 = self.get_close_async(body)
        coro2 = self.get_event_async(RpcTopic.Adjustment, body)
        coro3 = self.get_event_async(RpcTopic.Rightment, body)
        
        close, adj, rgt = await asyncio.gather(coro1, coro2, coro3)
        print("get_factor close ", close)
        print("get_factor adj ", adj)
        print("get_factor rgt ", rgt)
        factors = calc_factor(close, adj, rgt)
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


cdef class TdApi:
    """
    # How to implement a tradeApi:
    ---
    ## Basics
    A tradeApi should satisfies:
    * this class should be thread-safe:
        * all methods should be thread-safe
        * no mutable shared properties between objects.
    * all methods should be non-blocked
    * satisfies all requirements written in docstring for every method and callbacks.
    * automatically reconnect if connection lost.

    All the XxxData passed to callback should be constant, which means that
        the object should not be modified after passing to on_xxxx.
    So if you use a cache to store reference of data, use copy.copy to create a new object
    before passing that data into on_xxxx
    """
    
    def __init__(self, 
                    bytes client_id, 
                    tuple addr=("127.0.0.1", 9999), 
                    int timeout=5):
        self.client_id = client_id
        self.async_client = AsyncStreamClient(addr, timeout)

        loop, is_background = init_event_loop() # used for sync
        self.async_client.attach_loop(loop, is_background=is_background)
    
    def __enter__(self):
        return self 

    cdef object _send_request(self, int topic, bytes experiment_id=b'', object body=None, int sub_topic=-1):
            cdef bytes req_id = fast_uuid4_bytes()
            cdef object event = Event(
                topic=topic, 
                body=body, 
                experiment_id=experiment_id,
                sub_topic=sub_topic
            )
            return self.async_client.run(req_id, event)

# --------------------------------------------------------------- Ray Async Actor --------------------------------------------------------

    async def register_async(self, object body):
        fut = self._send_request(BrokerTopic.Register, experiment_id=b'', body=body)
        return await fut

    async def set_cash_async(self, bytes experiment_id, object body):
        fut = self._send_request(BrokerTopic.SetCash, experiment_id=experiment_id, body=body)
        return await fut

    async def getvalue_async(self, int topic, bytes experiment_id):
        fut = self._send_request(BrokerTopic.GetValue, experiment_id=experiment_id, body=None, sub_topic=topic)
        return await fut

    async def subscribe_async(self, int topic, bytes experiment_id, object body):
        fut = self._send_request(BrokerTopic.Subscribe, experiment_id=experiment_id, body=body, sub_topic=topic)
        return await fut

    async def submit_async(self, bytes experiment_id, object body):
        fut = self._send_request(BrokerTopic.Submit, experiment_id=experiment_id, body=body)
        return await fut

    async def on_dt_over_async(self, bytes experiment_id, object body):
        fut = self._send_request(BrokerTopic.DayOver, experiment_id=experiment_id, body=body)
        return await fut

# --------------------------------------------------------------- Sync and Local --------------------------------------------------------

    cpdef object register(self, object body):
        fut = self._send_request(topic=BrokerTopic.Register, experiment_id=b'', body=body)
        return fut.result()

    cpdef object set_cash(self, bytes experiment_id, object body): 
        fut = self._send_request(topic=BrokerTopic.SetCash, experiment_id=experiment_id, body=body)
        return fut.result()

    cpdef object getvalue(self, int topic, bytes experiment_id):
        fut = self._send_request(topic=BrokerTopic.GetValue, experiment_id=experiment_id, body=None, sub_topic=topic)
        return fut.result()
    
    cpdef object subscribe(self, int topic, bytes experiment_id, object body):
        fut = self._send_request(topic=BrokerTopic.Subscribe, experiment_id=experiment_id, body=body, sub_topic=topic)
        return fut.result()
    
    cpdef object submit(self, bytes experiment_id, object body):
        fut = self._send_request(topic=BrokerTopic.Submit, experiment_id=experiment_id, body=body)
        return fut.result()
    
    cpdef object on_dt_over(self, bytes experiment_id, object body):
        fut = self._send_request(topic=BrokerTopic.DayOver, experiment_id=experiment_id, body=body)
        return fut.result()

    cpdef void disconnect(self):
        self.async_client.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            print(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        self.async_client.close()


cdef class RpcApi:

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
        coro1 = self.get_close_async(body)
        coro2 = self.get_event_async(RpcTopic.Adjustment, body)
        coro3 = self.get_event_async(RpcTopic.Rightment, body)
        
        close, adj, rgt = await asyncio.gather(coro1, coro2, coro3)
        print("get_factor close ", close)
        print("get_factor adj ", adj)
        print("get_factor rgt ", rgt)
        factors = calc_factor(close, adj, rgt)
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

