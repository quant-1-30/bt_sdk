
cdef class MdApi:
    cdef object async_client

    cpdef object get_calendar(self)

    cpdef object get_instrument(self)

    cpdef object get_benchmark(self, object body)

    cpdef object get_event(self, str topic, object body)

    cpdef object subscribe(self, object body)

    cpdef object get_close(self, object body)
    
    cpdef object get_factor(self, object body)
    
    cpdef void disconnect(self)
