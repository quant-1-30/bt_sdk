# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid
import struct
from typing import Any



struct_format = {
    # tdapi
    "login": "!16s",
    # order_bit
    "order": "!iiff",
    "position": "!6sifiif16s",
    "account": "!iffii16s",
    "timer": "!f",
    # mdapi
    "calendar": "!I",
    "instrument": "!I",
    "tick": "!I",
    "adjustment": "!I",
    "rightment": "!I",
}
        
# json / pickle / msgpack / struct

def unpack(msg_type: str, msg: bytes) -> Any:
    if msg:
        unpacked = struct.unpack(struct_format[msg_type], msg)
        if msg_type == "login":
            uuid_obj = uuid.UUID(bytes=unpacked[0])
            return str(uuid_obj)
        return unpacked
