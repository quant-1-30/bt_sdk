#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from enum import Enum


class QuoteType(Enum):
    DEFAULT = ""
    INSTRUMENT = "instrument"
    CALENDAR = "calendar"
    DATASET = "dataset"
    TICK = "tick"
    ADJUSTMENT = "adjustment"
    RIGHT = "rightment"


class ApiEndpoint(Enum):
    LOGIN = "user/on_login"
    DEPLOY = "user/on_deploy"
    DISPLAY = "user/on_display"
    TRADE = "trade/on_trade"
    SYNC = "trade/on_sync"
    EVENT = "trade/on_event"
    ACCOUNT = "stats/on_account"
    METRICS = "stats/on_metrics"


class ApiMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    WS = "WS"
