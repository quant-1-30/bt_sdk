# /usr/bin/env python3
# -*- coding: utf-8 -*-

from bt_sdk.meta import MetaParams, with_metaclass
from bt_sdk.core.client.async_client import AsyncDatagramClient, AsyncStreamClient
from bt_sdk.core.model import ReqMeta, AuthMeta, RequestMsg


class MetaApi(MetaParams):
    
    def donew(cls, *args, **kwargs):
        """
            async_client / addr / client_id
        """
        _obj, args, kwargs = super(MetaApi, cls).donew(*args, **kwargs)
        
        if not hasattr(_obj, "get_data"):
            raise ValueError("get_data method is not implemented")

        async_client = AsyncDatagramClient if _obj.p.protocol == "udp" else AsyncStreamClient
        _obj.async_client = async_client(addr=_obj.p.addr)
        return _obj, args, kwargs


class Api(with_metaclass(MetaApi, object)):
    params = (("client_id", ""), ("addr", ""))
    
    def __enter__(self):
        return self
    
    def get_data(self, q):
        data = []
        while True:
            item = q.get()
            if item == "eof":
                break
            data.append(item)
        return data
    
    def get_iter_data(self, q):
        while True:
            item = q.get()
            if item == "eof":
                break
            yield item

    def on_request(self, topic: str, msg: ReqMeta, auth: AuthMeta=AuthMeta()):
        """
            request
        """
        msg = RequestMsg(topic=topic, msg=msg, auth=auth)
        q = self.async_client.run(msg.model_dump())
        data = self.get_data(q)
        return data

    def __exit__(self, exc_type, exc_value, traceback):
        self.async_client.stop()
