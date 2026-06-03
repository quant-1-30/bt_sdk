# distutils: language = c++
# cython: language_level=3

from libc.stdint cimport uint8_t


cdef struct UuidFrame:
    uint8_t data[16]


cdef UuidFrame fast_uuid4_c() nogil

cpdef str fast_uuid4_str()

cpdef bytes fast_uuid4_bytes()

cpdef object _merge2DataFrame(list batches, bint is_group=?) # cdef reduce python overhead
