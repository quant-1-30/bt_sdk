#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum

class MdMsgType(Enum):
    Calendar = 0
    Contract = 1
    Tick = 2
    Adjustment = 3
    Rightment = 4

class TdMsgType(Enum):
    Order = 0
    Request = 1
    Timer = 2

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

class Timer(Enum):
    daily = "daily"
    eos = "eos"


# class ContractType(Enum):
#     Future = 0
#     Option = 1
#     Stock = 2
#     Index = 3
#     ETF = 4
