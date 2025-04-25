#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum


class MdMsgType(Enum):
    Calendar = 0
    Contract = 1
    Tick = 2
    Adjustment = 3
    Rightment = 4

# instrument
class ContractType(Enum):
    Future = 0
    Option = 1
    Stock = 2
    Index = 3
    ETF = 4


class TdMsgType(Enum):
    Order = 0
    Request = 1
    Timer = 2

class ExecType(Enum):
    Market = 0
    Close = 1
    Limit = 2
    Stop = 3
    StopLimit = 4
