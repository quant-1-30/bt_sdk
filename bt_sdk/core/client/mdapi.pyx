# cython: language_level=3

import msgspec
import reactivex.operators as ops
import pyarrow as pa
# from contextlib import contextmanager

from bt_sdk.core.protocol import Event
from bt_sdk.core.client.async_client cimport AsyncZmqClient 
from bt_sdk.core.client.util cimport fast_uuid4_bytes


cdef inline object obs2table(object observable):
    cdef list result
    result = observable.pipe(
        ops.to_list() # ops.to_list() pack into list end of 
    ).run()
    if result:
        # return pa.Table.from_batches(result)  # pa.RecordBatch
        return pa.concat_tables(result) # zero_copy accumlate chunk ptr not reallocate / just when combine_chunks() 
    return {}


cdef class MdApi:

    def __init__(self,
                    tuple addr=("127.0.0.1", 8888),
                    int timeout = 5):
        self.async_client = AsyncZmqClient(addr=addr, timeout=timeout)

    def __enter__(self):
        return self 

    cpdef object get_calendar(self): 
        """
            request calendar
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Calendar)

        obs = self.async_client.run(req_id, event)
        return obs
    
    cpdef object get_instrument(self):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Instrument)

        obs = self.async_client.run(req_id, event)
        return obs
    
    cpdef object get_benchmark(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Index, body=body)

        obs = self.async_client.run(req_id, event)
        return obs 
    
    cpdef object subscribe(self, object body):
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Tick, body=body)

        obs = self.async_client.run(req_id, event)
        return obs
        
    cpdef object get_close(self, object body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=RpcTopic.Close, body=body)
        
        obs = self.async_client.run(req_id, event)
        return obs

    cpdef object get_event(self, int topic, object body):
        """
            request instruments
        """
        cdef bytes req_id = fast_uuid4_bytes()
        cdef object event = Event(topic=topic, body=body)
        
        obs = self.async_client.run(req_id, event)
        return obs

    cpdef object get_factor(self, object body):
        cdef object obs_close, obs_adjust, obs_right
        cdef object close, adjust, right

        obs_close = self.get_close(body)
        obs_adjust = self.get_event(RpcTopic.Adjustment, body)
        obs_right = self.get_event(RpcTopic.Rightment, body)
        close = obs2table(obs_close)
        adjust = obs2table(obs_adjust)
        right = obs2table(obs_right)
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
