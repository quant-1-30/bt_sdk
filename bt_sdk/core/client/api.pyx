# cython: language_level=3

import asyncio
import msgspec
import logging
import reactivex.operators as ops
import pyarrow as pa
import threading
import polars as pl

from bt_sdk.core.factor import calc_factor, calc_factor_async
from bt_sdk.core.client.async_client cimport AsyncRpcClient
from bt_sdk.utils.util cimport fast_uuid4_bytes, _merge2DataFrame

from bt_protocol._protocol import Event
from bt_protocol.constant import FactorTopic, RpcTopic

from libc.stdint cimport int32_t

logger = logging.getLogger(__name__)

cdef dict _md_api_registry = {}
cdef object _md_api_lock = threading.Lock()


cdef inline object scale(dict data):
    cdef object table = data["data"]
    return table


async def _collect_async(observable, timeout, subject=None):
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
        ops.map(scale),
        ops.share()
    ).subscribe(  # nonblocking
        on_next=on_next,
        on_error=on_error,
        on_completed=on_completed
    )
    try:
        await asyncio.wait_for(fut, timeout=timeout)
        return buffer
    except asyncio.TimeoutError:
        logger.error(f"[MdApi] Timeout collecting data after {timeout}s")
        raise
    except Exception as e:
        logger.exception(f"[MdApi] Error collecting data: {e}")
        raise
    finally:
        subscription.dispose()
        if subject is not None and not subject.is_disposed:
            subject.dispose()  # release observer chain to prevent leak


cdef class MdApi:

    def __init__(self, tuple addr, int32_t timeout):
        self.async_client = AsyncRpcClient(addr=addr, timeout=timeout)
        self.timeout = timeout
        self.loop = None
        self._is_initialized = False

    cpdef start(self, object loop):  # avoid loop is dead but is still initialized
        # Revive a client closed via __exit__ while still cached in the global
        # registry (GetMdApi returns a singleton per addr); without this the
        # instance stays dead ("client is not running") forever.
        # NOTE: local typed var — self.async_client is a plain object attribute,
        # so ._running must be reached via C-level access on the cimported type.
        cdef AsyncRpcClient client = self.async_client
        if not client._running:
            client._running = True

        if self._is_initialized:
            if self.loop is loop and not self.loop.is_closed():
                return
            else:
                logger.info(f"[MdApi] Old loop is dead or changed. Re-attaching...")
                # reset stale connection so a new gRPC channel is built on the new loop
                self.async_client.reset_connection()

        self.loop = loop
        logger.info(f"[MdApi] Attaching to Loop: {id(self.loop)}")
        self.async_client.attach_loop(self.loop)  # reuse main loop avoid cross thread
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

        obs, subject = self.async_client.run(req_id, event)
        tables = await _collect_async(obs, self.timeout, subject)
        data_df = _merge2DataFrame(tables, is_group=False)
        return data_df

    async def get_factor_async(self, object body, int32_t forward):
        # close
        close_obs, close_subj = self.async_client.run(fast_uuid4_bytes(), Event(topic=RpcTopic.Close, body=body))
        cdef object coro1 = _collect_async(close_obs, self.timeout, close_subj)

        # adjustment
        adj_obs, adj_subj = self.async_client.run(fast_uuid4_bytes(), Event(topic=RpcTopic.Adjustment, body=body))
        cdef object coro2 = _collect_async(adj_obs, self.timeout, adj_subj)

        # rightment
        rgt_obs, rgt_subj = self.async_client.run(fast_uuid4_bytes(), Event(topic=RpcTopic.Rightment, body=body))
        cdef object coro3 = _collect_async(rgt_obs, self.timeout, rgt_subj)

        # calculate
        close_tables, adj_tables, rgt_tables = await asyncio.gather(coro1, coro2, coro3)
        factors = await calc_factor_async(_merge2DataFrame(close_tables), _merge2DataFrame(adj_tables), _merge2DataFrame(rgt_tables), forward)
        return factors

    # ==============================================================
    #  Support Cross Grpc Loop for Direct Query
    # ==============================================================
    async def rpc_async(self, object body, int32_t rpc_type, int32_t timeout=30):
        if self.loop is None:
            raise RuntimeError(
                "[MdApi] rpc_async called before start(loop) — no event loop attached. "
                "Create the api via external_mdapi_context() or call start(loop) first."
            )

        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=rpc_type, body=body)
        cdef object tables

        # wrap grpc logic
        async def _internal_run():
            return await self.async_client.direct_run_async(req_id, event)

        try:
            curr_loop = asyncio.get_running_loop()
            if curr_loop is self.loop:
                tables = await asyncio.wait_for(_internal_run(), timeout=timeout)
            else:
                fut = asyncio.run_coroutine_threadsafe(
                    asyncio.wait_for(_internal_run(), timeout=timeout),
                    self.loop
                )
                # wrap_future ---> asyncio.Future
                tables = await asyncio.wrap_future(fut)  # future.result() blocking
        except RuntimeError:
            raise RuntimeError("[MdApi] rpc_async must be awaited inside a running event loop!")

        cdef bint is_group = False if rpc_type == RpcTopic.Instrument else True
        df = _merge2DataFrame(tables, is_group)
        return df

 # --------------------------------------------------------------- Sync Api --------------------------------------------------------

    cpdef object get_instrument(self, int32_t timeout=60):
        coroutine = self.get_instrument_async()
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result(timeout=timeout)

    cpdef object get_factor(self, object body, int32_t forward_type, int32_t timeout=60):
        coroutine = self.get_factor_async(body, forward_type)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result(timeout=timeout)

    cpdef object subscribe(self, object body, int32_t topic):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)

        result = self.async_client.run(req_id, event)
        # subscribe returns (observable, subject); return observable for streaming
        if isinstance(result, tuple):
            return result[0]
        return result

    cpdef void disconnect(self):
        self.async_client.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        self.async_client.close()


cpdef MdApi GetMdApi(tuple addr, int32_t timeout=30):
    global _md_api_registry

    cdef MdApi instance

    with _md_api_lock:
        if addr in _md_api_registry:
            return _md_api_registry[addr]

        logger.info(f"[MdApi] Initializing new instance for target: {addr}")
        instance = MdApi(addr=addr, timeout=timeout)
        _md_api_registry[addr] = instance

    return _md_api_registry[addr]


cdef void destroy(tuple addr):
    """destroy addr MdApi and release gRPC channel """
    global _md_api_registry

    with _md_api_lock:
        instance = _md_api_registry.pop(addr, None)
        if instance is not None:
            try:
                instance.disconnect()
            except Exception as e:
                logger.warning(f"[MdApi] Dispose error for {addr}: {e}")


cpdef void dispose():
    global _md_api_registry

    with _md_api_lock:
        addrs = list(_md_api_registry.keys())
    for addr in addrs:
        destroy(addr)
    