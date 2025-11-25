# /usr/bin/env python3
# -*- coding: utf-8 -*-

from contextlib import contextmanager
from typing import List, Mapping, Union, Generator, Any

from bt_sdk.core.client.api import Api
from bt_sdk.core.model import Query, Request


class MdApi(Api):
    params = (
            ("protocol", "zmq"),
            ("timeout", -1),  # Default timeout
        )

    def get_calendar(self) -> List[int]: 
        """
            request calendar
        """
        msg = Request(topic='calendar', body=Query())
        chan = self.get_channel()
        # import pdb; pdb.set_trace()
        self.async_client.run(msg.model_dump_json(), chan)
        cals = self.get_data(chan)
        return cals
    
    def get_instrument(self) -> List[Mapping[str, Any]]:
        """
            request instruments
        """
        rq = Request(topic='instrument', body=Query())
        chan = self.get_channel()
        self.async_client.run(rq.model_dump_json(), chan)
        instruments = self.get_data(chan)
        return instruments 
    
    def get_benchmark(self, index='000001') -> List[Mapping[str, Any]]:
        """
            request index 000001 000680 399006 399001
        """
        rq = Request(topic='index', body=Query(sid=[index]))
        chan = self.get_channel()
        self.async_client.run(rq.model_dump_json(), chan)
        data = self.get_data(chan)
        index = [item["body"]["line"][0] for item in data]
        return index 
    
    def get_event(self, topic, body: Query) -> List[Mapping[str, Any]]:
        """
            request instruments
        """
        rq = Request(topic=topic, body=body)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump_json(), chan)
        events = self.get_data(chan)
        return events 

    # @contextmanager
    # def subscribe(self, body: Query) -> Generator:
    def subscribe(self, body: Query):
        """使用迭代器模式替代上下文管理器"""
        rq = Request(topic='tick', body=body)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        
        def _iterator():
            try:
                while True:
                    data = chan.get()
                    if data == "eof" or data is None:
                        break
                    yield data
            finally:
                self.cancel(chan)
        
        return _iterator()
     
    def get_close(self, body: Query) -> List[Mapping[str, Any]]:
        """
            request instruments
        """
        rq = Request(topic='close', body=body)
        chan = self.get_channel()
        self.async_client.run(rq.model_dump(), chan)
        closes = self.get_data(chan)
        return closes

    def factor(self, body: Query) -> List[Any]: # sid
        close = self.get_close(body)
        adjust = self.get_event("adjustment", body)
        right = self.get_event("rightment", body)
        from bt_sdk.core.helper.factor import calc_factor
        factors = calc_factor(close, adjust, right)
        return factors
    
    def cancel(self, q):
        super().cancel(q)
    

__all__ = ["MdApi"]
