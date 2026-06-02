# cython: language_level=3

import asyncio
import msgspec
import reactivex.operators as ops
import pyarrow as pa
import threading
import polars as pl

from bt_sdk.core.protocol import Event
from bt_sdk.core.factor import calc_factor, apply_factor
from bt_sdk.core.client.async_client cimport AsyncRpcClient
from bt_sdk.utils.util cimport fast_uuid4_bytes, _merge2DataFrame

from libc.stdint cimport int32_t

cdef dict _md_api_registry = {} 
cdef object _md_api_lock = threading.Lock()


cdef inline object scale(dict data):
    cdef object table = data["data"]
    return table


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

    subscription = observable.pipe(
        # ops.sample(0.1),  # 100ms abandon reset 
        # ops.buffer_with_time_or_count(timespan=1.0, count=500), # up to 500 / 1 second to list
        # ops.throttle_first(0.05), # on receive / 50ms not receive
        # ops.publish_replay(1), # cache 1 record 
        # ops.ref_count()
        ops.map(scale),
        ops.share()
    ).subscribe( # nonblocking
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
 
    cpdef start(self, object loop): # avoid loop is dead but is still initialized
        if self._is_initialized:
            if self.loop is loop and not self.loop.is_closed():
                return  
            else:
                print(f"[MdApi] Old loop is dead or changed. Re-attaching...")
                
        self.loop = loop
        print(f"[MdApi] Attaching to Loop: {id(self.loop)}")
        self.async_client.attach_loop(self.loop) # reuse main loop avoid cross thread
        self._is_initialized = True
 
    def __enter__(self):
        return self
        
# --------------------------------------------------------------- Async Api --------------------------------------------------------
    
    async def get_instrument_async(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Instrument)

        obs = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout)
        data_df = _merge2DataFrame(tables, is_group=False)
        return data_df

    async def get_factor_async(self, object body, int32_t forward):
        # close
        close_obs = self.async_client.run(fast_uuid4_bytes(), Event(topic=RpcTopic.Close, body=body))
        cdef object coro1 = _collect_async(close_obs, self.timeout)

        # adjustment
        adj_obs = self.async_client.run(fast_uuid4_bytes(), Event(topic=RpcTopic.Adjustment, body=body))
        cdef object coro2 = _collect_async(adj_obs, self.timeout)
        
        # rightment
        rgt_obs = self.async_client.run(fast_uuid4_bytes(), Event(topic=RpcTopic.Rightment, body=body))
        cdef object coro3 = _collect_async(rgt_obs, self.timeout)

        # calculate  
        close_tables, adj_tables, rgt_tables = await asyncio.gather(coro1, coro2, coro3)
        factors = calc_factor(_merge2DataFrame(close_tables), _merge2DataFrame(adj_tables), _merge2DataFrame(rgt_tables), forward)
        return factors 

    async def rpc_async(self, object body, int32_t rpc_type, int32_t timeout=30):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=rpc_type, body=body)

        coro = self.async_client.direct_run_async(req_id, event)
        
        tables = await asyncio.wait_for(coro, timeout=timeout)
        
        cdef bint is_group = False if rpc_type == RpcTopic.Instrument else True
        df = _merge2DataFrame(tables, is_group)
        return df

# --------------------------------------------------------------- Sync Api --------------------------------------------------------

    cpdef object get_instrument(self):
        coroutine = self.get_instrument_async()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()
    
    cpdef object get_factor(self, object body, int32_t forward_type):
        coroutine = self.get_factor_async(body, forward_type)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result()
    
    cpdef object subscribe(self, object body, int32_t topic):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)

        obs = self.async_client.run(req_id, event)
        return obs

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
