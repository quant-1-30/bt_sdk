# /usr/bin/env python3
# -*- coding: utf-8 -*-

import threading

from bt_sdk.core.meta import with_metaclass, MetaSingleton
from bt_sdk.core.client.async_client import AsyncZmqClient, AsyncStreamClient
from bt_sdk.utils.wrapper import retry_connection
from bt_sdk.utils.net_util import on_ping
from bt_sdk.core.com import _Poll


class MetaApi(MetaSingleton):
    
    def donew(cls, *args, **kwargs):
        """
            async_client / addr
        """
        _obj, args, kwargs = super(MetaApi, cls).donew(*args, **kwargs)
        
        async_client = AsyncStreamClient if _obj.p.protocol == "tcp" else AsyncZmqClient
        _obj.async_client = async_client(addr=_obj.p.addr, timeout=_obj.p.timeout)

        # initialize poll 
        _obj._poll_event = threading.Event()
        _obj.poll = _Poll(pool_size=_obj.p.pool_size)
        return _obj, args, kwargs
    
    def dopostinit(cls, _obj, *args, **kwargs):
        _obj, args, kwargs = super(MetaApi, cls).dopostinit(_obj, *args, **kwargs)
        _obj._post_init()
        _obj.experiment_id = '' # record 
        return _obj, args, kwargs
    
   
class Api(with_metaclass(MetaApi, object)):

    params = (
        ("addr", ("127.0.0.1", 8888)),  # Default address tuple
        ("protocol", ""),
        ("pool_size", 10),  # Add pool size parameter
        ("checksum", "eof"),
        ("timeout", -1),  # Default timeout for queue operations
        ("unit", "ms"),
    )
    
    def _post_init(self):
        # Initialize cycle thread
        self._poll_thread = threading.Thread(
            target=self.poll.poll,
            daemon=True,  # 使用 daemon 线程，允许程序在中断时退出
            # daemon=False, # 使用非守护线程，确保资源被正确清理 / wait 会阻塞
            name=f"poll_worker_{id(self)}"
        )
        self._poll_thread.start()
    
    def get_channel(self):
        chan = self.poll.getTickQueue()
        return chan
 
    @retry_connection(max_attempts=3, delay=1)
    def connected(self):
        """
            self.async_client.connected on ping
        """
        return on_ping(self.p.addr[0], self.p.unit)
     
    def get_data(self, q): # queue.Empty
        data = []
        while True:
            msg = q.get(self.p.timeout)
            if msg == self.p.checksum:  # EOF
                self.cancel(q)
                break
            data.append(msg)
        return data
    
    def cancel(self, q):
        """
        Cancel data subscription by putting EOF into the queue and marking it for reuse.
        The queue will be available for reuse only after it's fully consumed.
        """
        self.poll.cancel(q)
    
    def disconnected(self):
        """
        disconnect from the server - optimized cleanup with sharded locks
        """
        try:
            self._poll_event.set()
            if hasattr(self, '_poll_thread') and self._poll_thread.is_alive():
                self._poll_thread.join(timeout=1.0)
            if hasattr(self, 'async_client'):
                self.async_client.close()
            self.poll.dispose()
        except:
            pass  # 忽略清理时的错误
