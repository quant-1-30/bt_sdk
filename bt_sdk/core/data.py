#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass


__all__ = ["OrderBit", "Position", "Account"]


@dataclass(frozen=True)
class Order:

    sid: str
    size: int
    price: int
    pricelimit: int
    exec_type: int
    order_type: int
    created_at: int
    sizer_ratio: float


@dataclass(frozen=True)
class OrderBit:

    executed_at: int
    executed_size: int
    executed_price: float
    comm: float
    direction: bool


@dataclass(frozen=True)
class Position:

    sid: str
    datetime: int
    size: int
    available: int
    price: float
    pnl: float
    experiment_id: str


@dataclass(frozen=True)
class Account:

    datetime: int
    portfolio_value: float
    cash: float
    pnl: float
    leverage: int
    margin: float
    experiment_id: str
