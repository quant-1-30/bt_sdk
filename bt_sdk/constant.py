#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum, auto

CHUNK_HEADER_FORMAT = ">HB"


class OrderType(Enum):
    Buy = 0 # auto default 1 
    Sell = auto()


class ExecType(Enum):
    Market = 0
    Limit = auto()
    Stop = auto()
    StopLimit = auto()
