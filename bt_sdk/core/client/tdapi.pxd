cdef class TdApi:
    cdef bytes client_id
    cdef object async_client

    cpdef object register(self, object body)

    cpdef object set_cash(self, bytes experiment_id, object body)

    cpdef object getvalue(self, bytes experiment_id, bytes req_type)

    cpdef object subscribe(self, bytes experiment_id, object body)

    cpdef object submit(self, bytes experiment_id, object body)

    cpdef object on_dt_over(self, bytes experiment_id, object body)

    cpdef void disconnect(self)
