from libc.stdint cimport int32_t

cpdef enum RpcTopic:
    Calendar = 0
    Instrument = 1
    Index = 2
    Tick = 3 
    Close = 4
    Adjustment = 5
    Rightment = 6
 

cpdef enum FactorTopic:
    Raw = 0
    Qfq = 1
    Hfq = 2


cdef class MdApi:
    cdef bint _is_initialized
    cdef int32_t timeout
    cdef public object loop
    cdef object async_client
    
    cpdef start(self, object loop)

    cpdef object get_calendar(self)

    cpdef object get_instrument(self)

    cpdef object get_benchmark(self, object body)

    cpdef object get_subscribe(self, object body, int32_t forward)
    
    cpdef object get_close(self, object body)
    
    cpdef object get_event(self, int topic, object body)

    cpdef object get_factor(self, object body, int32_t forward)
    
    cpdef void disconnect(self)
