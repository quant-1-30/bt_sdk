#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass


__all__ = ["Resp", "Trade", "Position", "Account"]


@dataclass(frozen=True)
class Resp:
    body: dict


@dataclass(frozen=True)
class Trade:
    executed_dt: int
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
    cost_basis: float
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
