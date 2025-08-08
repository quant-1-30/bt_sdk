from ping3 import ping


def on_ping(addr, unit):
    """
    ping the server
    """
    resp = ping(dest_addr=addr, timeout=2.0, unit=unit)
    if resp is False:
        raise ValueError("domain not found")
    elif resp is None:
        raise ValueError("ping timeout")
    else:
        return resp
