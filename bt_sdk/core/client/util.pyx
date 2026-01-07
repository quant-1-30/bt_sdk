# cython: language_level=3

from ping3 import ping

from libc.stdint cimport uint8_t

# C typedef / cython ctypedef
cdef extern from "<uuid/uuid.h>" nogil:
    ctypedef uint8_t uuid_t[16]
    void uuid_generate(uuid_t out)
    void uuid_unparse_lower(const uuid_t uu, char *out)


cpdef bytes fast_uuid4_bytes():
    """
        16 字节的原始 UUID 二进制流
    """
    cdef uuid_t uu
    with nogil:
        uuid_generate(uu)
    
    # 将 16 字节内存直接转为 Python bytes
    return (<char*>uu)[:16]


cpdef str fast_uuid4_str():
    """
        UUID str
    """
    cdef uuid_t uu
    cdef char out[37] # 36字符 + \0
    with nogil:
        uuid_generate(uu)
        uuid_unparse_lower(uu, out)
    return out[:36].decode('ascii')


cpdef double on_ping(str addr, int timeout , str unit): # unit = s means second
    """
        ping the server
    """
    cdef double resp
    resp = ping(dest_addr=addr, timeout=timeout, unit=unit)
    if resp is False:
        raise ValueError("domain not found")
    elif resp is None:
        raise ValueError("ping timeout")
    else:
        return resp
