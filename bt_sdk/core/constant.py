#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum


CHUNK_HEADER_FORMAT = ">HB"


class OrderType(Enum):
    Buy = 0
    Sell = 1


class ExecType(Enum):
    Open = 0
    Market = 1
    Close = 2
    Limit = 3
    Stop = 4
    StopLimit = 5


# class ContractType(Enum):
#     Future = 0
#     Option = 1
#     Stock = 2
#     Index = 3
#     ETF = 4
