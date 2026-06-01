from libc.stdint cimport int32_t

cpdef enum RpcTopic:
    Instrument = 0
    Tick = 1
    Close = 2
    Adjustment = 3
    Rightment = 4
 

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

    cpdef object get_instrument(self)

    cpdef object get_factor(self, object body, int32_t forward)
    
    cpdef object subscribe(self, object body, int32_t topic)
    
    cpdef void disconnect(self)
