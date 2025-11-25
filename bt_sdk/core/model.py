#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import pydantic
import hashlib
from datetime import datetime
from uuid import UUID, uuid4
from pydantic import Field, ConfigDict, computed_field, field_validator, field_serializer
from typing import List, Union, Any, Dict


__all__ = ["Experiment", "Cash",  "Order", "Query", "Request"]


class Experiment(pydantic.BaseModel):
    strategy: str
    extra_info: str
    client_id: str
    
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    @computed_field
    @property
    def identity(self) -> bytes:
        identity_str = f"{self.strategy},{self.extra_info}"
        md5_obj = hashlib.md5()
        md5_obj.update(identity_str.encode("utf-8"))
        return md5_obj.digest()
    
    def __repr__(self): # __repr__ / __str__
        return f"Experiment(strategy={self.strategy!r}, extra_info={self.extra_info!r}, client_id={self.client_id!r})" 


class Cash(pydantic.BaseModel): # 
    session: datetime
    cash: int
    
    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    @field_validator('session', mode='before') # validate return datetime and seperate from serialize
    def validate_date(cls, v):
        if isinstance(v, datetime):
            return v
        else:
            v = datetime.strptime(str(v), "%Y%m%d")
        return v
    
    @field_serializer("session")
    def serialize_session(self, session: datetime, _info) -> int:
        """序列化：将整数转换回日期字符串"""
        # session_int = int(session.strftime("%Y%m%d")) 
        session_timestamp = int(session.timestamp()) # datetime -> timestamp 自动处理utc转换 / timestamp ---> datetime 需要转时区
        return session_timestamp

    def __repr__(self): # __repr__ / __str__
        return f"Cash(session={self.session!r}, cash={self.cash})" 


class Order(pydantic.BaseModel):
    sid: str
    pricelimit: int
    sizer_ratio: int
    order_type: int
    exec_type: int
    created_dt: int

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    def __repr__(self): # __repr__ / __str__
        return f"Order(sid={self.sid!r}, pricelimit={self.pricelimit!r}, exec_type={self.exec_type!r}, \
            order_type={self.order_type!r}, created_at={self.created_dt!r}, sizer_ratio={self.sizer_ratio!r})" 


class Query(pydantic.BaseModel):
    start_date: datetime = Field(default=datetime(1990, 1, 1))
    end_date: datetime = Field(default=datetime.now())
    sid: List[str] = Field(default=[])
    topic: str = Field(default="")

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    @field_validator('start_date', 'end_date', mode="before")
    def validate_date(cls, v):
        if isinstance(v, datetime):
            return v
        elif isinstance(v, str): 
            v = datetime.strptime(v, '%Y%m%d')
            return v
        elif isinstance(v, int) and v > 0:
            v = datetime.fromtimestamp(v) if len(str(v)) > 8 else datetime.strptime(str(v), '%Y%m%d') 
            return v
        else:
            raise TypeError(f"Unsupported type for date: {type(v)}. Expected str, int, float, or datetime.")
    
    # @field_serializer("start_date", "end_date", when_used="json") # always
    # def serialize_dt(self, v: datetime, _info) -> int:
    #     return int(v.strftime("%Y%m%d"))
    
    @field_serializer("start_date", "end_date")
    def serialize_dt(self, v: datetime, info) -> int:
        # 根据序列化模式决定格式
        mode = getattr(info, 'mode', 'python')
        
        if mode == 'json':
            return int(v.strftime("%Y%m%d"))  # JSON 使用 YYYYMMDD
        else:
            return int(v.timestamp())  # 其他情况使用时间戳
             
    def __repr__(self): # __repr__ / __str__
        return f"Query(sid={self.sid!r}, start_date={self.start_date!r}, end_date={self.end_date!r}, topic={self.topic!r})" 


class Request(pydantic.BaseModel):

    topic: str
    body: Union[Experiment, Cash, Order, Query]
    request_type: str = Field(default="")
    experiment_id: str = Field(default="") # default is used for mdapi
    request_id: UUID = Field(default_factory=uuid4)


    def model_dump(self, *args, **kwargs) -> Dict[str, Any]:
       data = super().model_dump(*args, **kwargs)
       if 'request_id' in data and isinstance(data['request_id'], UUID):
           data['request_id'] = str(data['request_id'])        
       return data

    model_config = ConfigDict(
        extra="forbid",
        frozen=True
    )

    def __repr__(self): # __repr__ / __str__
        return f"Request(topic={self.topic!r}, body={self.body!r}, experiment_id={self.experiment_id!r}, request_id={self.request_id!r})"
