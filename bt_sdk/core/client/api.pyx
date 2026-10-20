# cython: language_level=3, boundscheck=False, wraparound=False

import asyncio
import logging
import threading

from bt_sdk.core.factor import calc_factor_async
from bt_sdk.core.client.async_client cimport AsyncRpcClient
from bt_sdk.utils.util cimport fast_uuid4_bytes, _merge2DataFrame

from bt_protocol._protocol import Event, QueryBody
from bt_protocol.constant import RpcTopic

from libc.stdint cimport int32_t

logger = logging.getLogger(__name__)

cdef dict _md_api_registry = {}
cdef object _md_api_lock = threading.Lock()

# Full-history valid YYYYMMDD bounds (A-share trading began 1990-12-19; server
# casts bounds to TIMESTAMPTZ, so sentinel ints like 0 / MaxDate are invalid).
_FULL_HISTORY_START = 19900101
_FULL_HISTORY_END = 20991231


def _full_history_body(object body):
    """Widen a QueryBody to full listed history ([19900101, 20991231]) so hfq
    (anchored at listing) and qfq (anchored at latest price) include all events
    and their preclose rows. Non-QueryBody inputs pass through unchanged."""
    if not isinstance(body, QueryBody):
        return body
    return QueryBody(start_date=_FULL_HISTORY_START, end_date=_FULL_HISTORY_END, sid=body.sid)


cdef class MdApi:

    def __init__(self, tuple addr, int32_t timeout):
        self.async_client = AsyncRpcClient(addr=addr, timeout=timeout)
        self.timeout = timeout
        self.loop = None
        self._is_initialized = False
        self._is_shared = False

    cpdef start(self, object loop):
        self.async_client.revive()

        if self._is_initialized:
            if self.loop is loop and not self.loop.is_closed():
                return
            if self._is_shared and self.loop is not None and not self.loop.is_closed() and self.loop.is_running():
                logger.warning(
                    f"[MdApi] Shared singleton already attached to active loop "
                    f"{id(self.loop)}; ignoring re-attach request to {id(loop)}"
                )
                return
            logger.info("[MdApi] Old loop is dead or changed. Re-attaching...")
            self.async_client.reset_connection()

        self.loop = loop
        logger.info(f"[MdApi] Attaching to Loop: {id(self.loop)}")
        self.async_client.attach_loop(self.loop)
        self._is_initialized = True

    def __enter__(self):
        return self

# --------------------------------------------------------------- Async Api --------------------------------------------------------

    async def get_instrument_async(self, int32_t timeout=0):
        """Request instrument table (timeout <= 0 uses self.timeout)."""
        return await self.rpc_async(None, RpcTopic.Instrument, timeout=timeout)

    async def get_factor_async(self, object body, int32_t forward, int32_t timeout=0):
        """Fetch full-history close/adjustment/rightment concurrently and compute factors."""
        cdef object full_body = _full_history_body(body)
        closes, adjs, rgts = await asyncio.gather(
            self.rpc_async(full_body, RpcTopic.Close, timeout=timeout),
            self.rpc_async(full_body, RpcTopic.Adjustment, timeout=timeout),
            self.rpc_async(full_body, RpcTopic.Rightment, timeout=timeout),
        )
        return await calc_factor_async(closes, adjs, rgts, forward)

    async def rpc_async(self, object body, int32_t rpc_type, int32_t timeout=0):
        self._ensure_loop_attached()

        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=rpc_type, body=body)
        cdef int32_t collect_timeout = self.timeout if timeout <= 0 else timeout
        cdef object tables = await self.async_client.fetch_tables_async(req_id, event, collect_timeout)

        cdef bint is_group = rpc_type != RpcTopic.Instrument
        return _merge2DataFrame(tables, is_group)

    async def stream_async(self, object body, int32_t topic, int32_t max_queue=64):
        """Backpressure-aware async iterator yielding Arrow Table batches on
        the caller's own event loop."""
        self._ensure_loop_attached()
        cdef object event = Event(topic=topic, body=body)
        async for table in self.async_client.stream_batches_async(event, max_queue):
            yield table

    def _ensure_loop_attached(self):
        if self.loop is None or self.loop.is_closed() or not self.loop.is_running():
            raise RuntimeError(
                "[MdApi] no usable event loop attached (loop is None, closed, "
                "or not running). Create the api via external_mdapi_context() "
                "or call start(running_loop) first."
            )

 # --------------------------------------------------------------- Sync Api --------------------------------------------------------

    cpdef object get_instrument(self, int32_t timeout=60):
        self._ensure_loop_attached()
        coroutine = self.get_instrument_async(timeout=timeout)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result(timeout=timeout)

    cpdef object get_factor(self, object body, int32_t forward_type, int32_t timeout=60):
        self._ensure_loop_attached()
        coroutine = self.get_factor_async(body, forward_type, timeout=timeout)
        future = asyncio.run_coroutine_threadsafe(coro=coroutine, loop=self.loop)
        return future.result(timeout=timeout)

    cpdef object subscribe(self, object body, int32_t topic):
        self._ensure_loop_attached()

        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)

        result = self.async_client.run(req_id, event)
        if isinstance(result, tuple):
            return result[0]
        return result

    cpdef void disconnect(self):
        self.async_client.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"Error: {exc_type}, {exc_val}, {exc_tb}")
        # Shared registry singletons are cleaned up via disconnect()/DisposeMdApi()/dispose().
        if not self._is_shared:
            self.async_client.close()


cpdef MdApi GetMdApi(tuple addr, int32_t timeout=30):
    global _md_api_registry

    cdef MdApi instance

    with _md_api_lock:
        if addr in _md_api_registry:
            instance = _md_api_registry[addr]
            if timeout > 0 and instance.timeout != timeout:
                instance.timeout = timeout
                instance.async_client.timeout = timeout
            if instance.loop is not None and not instance.loop.is_closed() and instance.loop.is_running():
                instance.async_client.revive()
            return instance

        logger.info(f"[MdApi] Initializing new instance for target: {addr}")
        instance = MdApi(addr=addr, timeout=timeout)
        instance._is_shared = True
        _md_api_registry[addr] = instance

    return instance


cpdef void DisposeMdApi(tuple addr):
    """Dispose the MdApi cached for `addr` and release its gRPC channel."""
    global _md_api_registry

    with _md_api_lock:
        instance = _md_api_registry.pop(addr, None)

    if instance is not None:
        try:
            instance.disconnect()
        except Exception as e:
            logger.warning(f"[MdApi] Dispose error for {addr}: {e}")


cpdef void dispose():
    """Dispose and clear every cached MdApi instance."""
    global _md_api_registry

    with _md_api_lock:
        addrs = list(_md_api_registry.keys())
    for addr in addrs:
        DisposeMdApi(addr)
