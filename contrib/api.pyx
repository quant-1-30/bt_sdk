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

    async def getvalue_async(self, bytes experiment_id):
        fut = self._send_request(BrokerTopic.GetValue, experiment_id=experiment_id)
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

    cpdef object getvalue(self, bytes experiment_id):
        fut = self._send_request(topic=BrokerTopic.GetValue, experiment_id=experiment_id)
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
