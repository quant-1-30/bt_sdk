# /usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Tuple
from utils.wrapper import singleton
from .api import Api


# @singleton
class MdApi(Api):
    params = (("protocol", "udp"),)

    def __init__(self, addr: Tuple[str, int]=()):

        self.addr = addr

__all__ = ["MdApi"]
