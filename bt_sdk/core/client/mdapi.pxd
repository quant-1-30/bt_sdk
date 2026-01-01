
cdef class MdApi:
    cdef object async_client

    cpdef object get_calendar(self)

    cpdef object get_instrument(self)

    cpdef object get_benchmark(self, bytes index)

    cpdef object get_event(self, str topic, dict body)

    cpdef object subscribe(self, dict body)

    cpdef object get_close(self, dict body)
