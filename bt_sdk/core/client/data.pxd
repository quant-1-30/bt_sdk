from libcpp.string cimport string as cpp_string
from libc.stdint cimport int64_t


cdef struct Trade:
    int executed_dt
    int executed_size
    double executed_price
    double comm


cdef struct Position:
    cpp_string sid
    int64_t datetime
    int size
    int available
    int cost_basis
    double pnl
    cpp_string experiment_id


cdef struct Account:
    int64_t datetime
    double portfolio_value
    double cash
    double pnl
    int leverage
    double margin
    cpp_string experiment_id


cdef enum OrderType:
    Buy = 0 
    Sell = 1


cdef enum ExecType:
    Market = 0
    Limit = 1
    Stop = 2
    StopLimit = 3
