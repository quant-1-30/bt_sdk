cdef enum BrokerTopic:
    Register = 0
    SetCash = 1
    Submit = 2
    DayOver = 3 
    GetValue = 4
    Subscribe = 5

cpdef enum SubTopic:
    Order = 0
    Position = 1
    Account = 2

cpdef enum OrderType:
    Buy = 0
    Sell = 1
    Unkown = 2

cpdef enum ExecType:
    Market = 0
    Close = 1
    Limit = 2
    Stop = 3
    StopLimit = 4
    StopTrail = 5
    StopTrailLimit = 6
    Historical = 7


cdef class TdApi:
    cdef bytes client_id
    cdef object async_client

    cpdef object register(self, object body)

    cpdef object set_cash(self, bytes experiment_id, object body)

    cpdef object getvalue(self, bytes experiment_id, int topic)

    cpdef object subscribe(self, bytes experiment_id, int topic, object body)

    cpdef object submit(self, bytes experiment_id, object body)

    cpdef object on_dt_over(self, bytes experiment_id, object body)

    cpdef void disconnect(self)
