cdef class TdApi:
    cdef bytes client_id
    cdef object async_client

    cpdef object register(self, dict body)

    cpdef object set_cash(self, bytes experiment_id, dict body)

    cpdef object getvalue(self, bytes experiment_id, str req_type)

    cpdef object subscribe(self, bytes experiment_id, dict body)

    cpdef object submit(self, bytes experiment_id, dict body)

    cpdef object on_dt_over(self, bytes experiment_id, dict body)

    cpdef void close(self)
