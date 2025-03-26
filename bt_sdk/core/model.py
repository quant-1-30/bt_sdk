#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import pydantic
from enum import Enum
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any


class QuoteType(Enum):
    DEFAULT = ""
    INSTRUMENT = "instrument"
    CALENDAR = "calendar"
    DATASET = "dataset"
    TICK = "tick"
    ADJUSTMENT = "adjustment"
    RIGHT = "rightment"


class QuoteMeta(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
    end_date: int = Field(default=30000101)
    sid: List[str] = Field(default=[])

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class QuoteEvent(pydantic.BaseModel):
    rpc_type: QuoteType
    quote_meta: QuoteMeta

    @field_validator('rpc_type')
    def validate_rpc_type(cls, v):
        assert v in [QuoteType.DEFAULT, QuoteType.INSTRUMENT, QuoteType.CALENDAR, 
                     QuoteType.DATASET, QuoteType.TICK, QuoteType.ADJUSTMENT, QuoteType.RIGHT], "Invalid event type"
        return v
    
    @field_validator('quote_meta')
    def validate_quote_meta(cls, v):
        assert v.start_date <= v.end_date, "start_date must be less than end_date"
        return v

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    # _secret: str = "hidden"  # Private attribute
    # model_config = {"private_attributes": {"_secret"}}


class TradeType(Enum):
    Order = "order"
    Eos = "eos"
    Query = "query"


class ExecType(Enum):
    Market = 0
    Close = 1
    Limit = 2
    Stop = 3
    StopLimit = 4


class OrderMeta(pydantic.BaseModel):
    owner: str
    sid: str
    size: int = Field(default=0)
    price: int
    pricelimit: int
    created_at: str
    exectype: int

    @field_validator('exectype')
    def validate_exectype(cls, v):
        if v not in [ExecType.Market, ExecType.Close, ExecType.Limit, ExecType.Stop, ExecType.StopLimit]:
            raise ValueError('Invalid exectype')
        return v

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class EosMeta(pydantic.BaseModel):
     dteos: int
     closes: Mapping[str, float]
     adjustments: List[Mapping[str, float]]

     @field_validator('dteos')
     def validate_dteos(cls, v):
         if isinstance(v, str):
             v = datetime.strptime(v, "%Y%m%d%H%M%S")
         elif isinstance(v, datetime):
             v = int(v.timestamp())
         assert v > 0, "dteos must be greater than 0"
         return v

     class Config:
         extra = 'forbid'
         allow_mutation = False


class QueryMeta(pydantic.BaseModel):

    user_id: str
    start_dt: datetime
    end_dt: datetime
  
    class Config:
        extra = "forbid"   
        allow_mutation = False


class TradeEvent(pydantic.BaseModel):

    event_type: TradeType
    meta: Union[QueryMeta, OrderMeta, EosMeta]

    @field_validator('event_type')
    def validate_event_type(cls, v):
        assert v in [TradeType.Order, TradeType.Eos, TradeType.Query], "Invalid event type"
        return v



class LogEvent(pydantic.BaseModel):

    log_level: str
    log_meta: Any

    class Config:
        extra = "forbid"   
        allow_mutation = False


__all__ = ["QuoteEvent", "QuoteMeta", "QuoteType", "TradeEvent", "LogEvent", "OrderMeta", "EosMeta", "QueryMeta", "TradeType"]
