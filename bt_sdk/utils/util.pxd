# distutils: language = c++
# cython: language_level=3

from libc.stdint cimport uint8_t, int64_t
from libc.time cimport gmtime, time_t, tm


cdef struct UuidFrame:
    uint8_t data[16]


# cdef struct CTime: # replace datetime
#     int year, month, day, hour, minute, second, microsecond

cdef struct MarketTime:
    int64_t open_ts
    int64_t close_ts


# C typedef / cython ctypedef
cdef inline UuidFrame fast_uuid4_c() nogil

cpdef bytes fast_uuid4_bytes()

cpdef inline str fast_uuid4_str()

cdef object num2date(double x, bint native=?, bint ordinal=?)


cdef MarketTime market_utc(int64_t ts, bint native=?) nogil

 
cdef int64_t ts_to_int_date(int64_t ts, bint native=?) nogil


cpdef object _merge_tables(list batches, bint is_group=?) # cdef reduce python overhead
