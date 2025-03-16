#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
from pydantic import Field, field_validator, ConfigDict
from typing import List, Union, Tuple, Mapping, Any

class QuoteMeta(pydantic.BaseModel):
    start_date: int = Field(default=19900101)
    end_date: int = Field(default=30000101)
    sid: List[str] = Field(default=[])

    # class Config:
    #     extra = "forbid"   
    #     # allow_mutation = False
    #     frozen = True

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class LoginMeta(pydantic.BaseModel):
    name: str
    phone: int
    email: str

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    # _secret: str = "hidden"  # Private attribute
    # model_config = {"private_attributes": {"_secret"}}

class AuthMeta(pydantic.BaseModel):

    token: str  
    experiment_id: str = Field(default="")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )


class OrderMeta(pydantic.BaseModel):
    sid: str
    order_type: int
    direction: int
    created_at: str
    price: int
    amount: int = Field(default=0)
    size: int = Field(default=0)

    @field_validator('direction')
    def validate_direction(cls, v):
        if v not in [1, 0]:
            raise ValueError('Invalid order type')
        return v

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

# class AssetMeta(pydantic.BaseModel):
#     sid: str
#     first_trading: int
#     delist: int

#     class Config:
#         extra = 'forbid'
#         allow_mutation = False

# class TradeMeta(pydantic.BaseModel):

#     order_meta: OrderMeta
#     experiment_id: str
#     meta: Dict[str, Any]
    
#     class Config:
#         extra = "forbid"   
#         allow_mutation = False


class EventMeta(pydantic.BaseModel):

    event_type: str
    meta: QuoteMeta


class RangeMeta(pydantic.BaseModel):

    start_dt: int
    end_dt: int


# class TradeMeta(pydantic.BaseModel):
#     experiment_id: str
#     session_ix: str
#     sids: List[str]
    

# class TradeEvent(pydantic.BaseModel):
    
#     event_type: str
#     execution_style: str
#     trade_meta: TradeMeta
    
#     class Config:
#         extra = "forbid"   
#         allow_mutation = False

#     @field_validator(mode="before")
#     def validate_event_type(cls, v):
#         assert v in [TradeType.TRADE, TradeType.SYNC, TradeType.EVENT], "Invalid event type"
#         return v


# class Quote(pydantic.BaseModel):
#     event_type: QuoteType = QuoteType.DEFAULT
#     quote_meta: QuoteMeta
    
#     class Config:
#         extra = "forbid"   
#         allow_mutation = False

#     @field_validator(mode="before")
#     def validate_event_type(cls, v):
#         assert v in [QuoteType.DEFAULT, QuoteType.INSTRUMENT, QuoteType.CALENDAR, QuoteType.DATASET, QuoteType.TICK, QuoteType.ADJUSTMENT, QuoteType.RIGHT], "Invalid event type"
#         return v


# class LogEvent(pydantic.BaseModel):

#     log_level: str
#     log_meta: Any

#     class Config:
#         extra = "forbid"   
#         allow_mutation = False
