#! /usr/bin/env python3
# -*- coding: utf-8 -*-


cdef enum TickMultiply:
    tick = 1
    open = 100000
    high = 100000
    low = 100000
    close = 100000
    volume = 1000
    amount = 1000

cdef enum BenchMultiply:
    date = 1
    _open = 100
    _high = 100
    _low = 100
    _close = 100
    _volume = 1
    _amount = 1

cdef enum EventMultiply:
    Adjustment = 1000
    Right = 1000
