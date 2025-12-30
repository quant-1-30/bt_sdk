cdef class TdApi:
    cdef str client_id
    cdef object async_client

    cpdef object register(self, dict body)

    cpdef object set_cash(self, dict body)

    cpdef object getvalue(self, dict body)

    cpdef object subscribe(self, dict body)

    cpdef object submit(self, dict body)

    cpdef object on_dt_over(self, dict body)

    cpdef void close(self)
