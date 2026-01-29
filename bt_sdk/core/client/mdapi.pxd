cpdef enum RpcTopic:
    Calendar = 0
    Instrument = 1
    Index = 2
    Tick = 3 
    Close = 4
    Adjustment = 5
    Rightment = 6
 

cdef class MdApi:
    cdef object async_client
    cpdef object get_calendar(self)

    cpdef object get_instrument(self)

    cpdef object get_benchmark(self)

    cpdef object subscribe(self, object body)
    
    cpdef object _event_obs(self, int topic, object body)

    cpdef object _close_obs(self, object body)
    
    cpdef void disconnect(self)
