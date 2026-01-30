# cython: language_level=3

import msgspec
from contextlib import contextmanager

from bt_sdk.core.protocol import Event
from bt_sdk.core.client.util cimport fast_uuid4_bytes
from bt_sdk.core.client.async_client cimport AsyncStreamClient


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

        self.async_client.attch_loop()
    
    cpdef object register(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=BrokerTopic.Register, body=body)
        
        fut = self.async_client.run(req_id, event)
        return fut

    cpdef object set_cash(self, bytes experiment_id, object body): 
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=BrokerTopic.SetCash, experiment_id=experiment_id, body=body)

        fut = self.async_client.run(req_id, event)
        return fut

    cpdef object getvalue(self, bytes experiment_id, int topic):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=BrokerTopic.GetValue, sub_topic=topic, experiment_id=experiment_id)

        fut = self.async_client.run(req_id, event)
        return fut
    
    cpdef object subscribe(self, bytes experiment_id, int topic, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=BrokerTopic.Subscribe, sub_topic=topic, experiment_id=experiment_id, body=body)

        fut = self.async_client.run(req_id, event)
        return fut
    
    cpdef object submit(self, bytes experiment_id, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=BrokerTopic.Submit, experiment_id=experiment_id, body=body)

        fut = self.async_client.run(req_id, event)
        return fut
    
    cpdef object on_dt_over(self, bytes experiment_id, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=BrokerTopic.DayOver, experiment_id=experiment_id, body=body)
        
        fut = self.async_client.run(req_id, event)
        return fut
    
    cpdef void disconnect(self):
        self.async_client.close()
