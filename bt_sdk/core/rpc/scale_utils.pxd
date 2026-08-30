# scale_utils.pxd
from libc.math cimport round, log10

# pure C inline func --- zero python object / GIL
cdef inline int get_scale_ndigits(double factor) noexcept nogil:
    # Round at the exact stored precision of each scale factor (1e-5 -> 5 digits,
    # 1e-3 -> 3): removes float artifacts from int*factor WITHOUT discarding real
    # precision. A hard-coded ndigits=2 silently truncates rows whose stored value
    # has more than 2 decimals — verified live: ~1.5% of close rows are finer than
    # 2dp, so those would lose up to 0.005 yuan (large relative error on low-priced
    # instruments) before ever reaching factor computation.`
    if factor >= 1.0:
        return 0
    elif factor == 1e-3:
        return 3
    elif factor == 1e-5:
        return 5
    else:
        # 通用 fallback 计算
        return <int>round(-log10(factor))
