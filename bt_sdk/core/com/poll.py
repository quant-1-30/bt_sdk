# /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import itertools
import threading

from bt_sdk.meta import with_metaclass, MetaSingleton
from bt_sdk.core.com.pool import QueuePool

__all__ = ["_Poll"]


class MetaPoll(MetaSingleton):
    
    def donew(cls, *args, **kwargs):
        """
            async_client / addr / client_id
        """
        _obj, args, kwargs = super(MetaPoll, cls).donew(*args, **kwargs)

        # Initialize queue pool
        _obj._queue_pool = QueuePool(max_size=kwargs.get('pool_size', 10))
        
        # 分片锁优化: 将_active_q拆分为多个分片，减少锁竞争
        _obj._shard_count = max(4, os.cpu_count()) 
        _obj._active_q_shards = [set() for _ in range(_obj._shard_count)]
        _obj._shard_locks = [threading.RLock() for _ in range(_obj._shard_count)]
        
        _obj.nextTickerId = itertools.count(0)
        # Initialize poll event
        _obj._poll_event = threading.Event()
        _obj._dirty_shards = [False] * _obj._shard_count
        return _obj, args, kwargs
    
    def dopostinit(cls, _obj, *args, **kwargs):

        _obj, args, kwargs = super(MetaPoll, cls).dopostinit(_obj, *args, **kwargs)
        return _obj, args, kwargs
    

class _Poll(with_metaclass(MetaPoll, object)):

    params = (
        ("pool_size", 10),  # Add pool size parameter
        ("poll_interval", 0.1),  # Cycle check interval in seconds
    )
    
    def getTickQueue(self):
            '''Creates or reuses a Queue for data delivery to a data feed'''
            q = self._queue_pool.get()
            tickerId = next(self.nextTickerId)
            q.unique(tickerId)
            self._add_to_active_q(q)
            return q
    
    def _add_to_active_q(self, queue_obj):
        """将队列添加到对应的分片中"""
        shard_idx = self._get_shard_index(queue_obj)
        with self._shard_locks[shard_idx]:
            self._active_q_shards[shard_idx].add(queue_obj)
            self._dirty_shards[shard_idx] = True

    def _get_shard_index(self, queue_obj):
        """根据队列对象计算分片索引"""
        return abs(hash(id(queue_obj))) % self._shard_count 
   
    def _remove_from_active_q(self, queue_obj):
        """从对应的分片中移除队列"""
        shard_idx = self._get_shard_index(queue_obj)
        with self._shard_locks[shard_idx]:
            self._active_q_shards[shard_idx].discard(queue_obj)
            self._dirty_shards[shard_idx] = True
 
    def poll(self):
        """
        Cycle worker that checks and cleans up non-consumed queues.
        Runs in a daemon thread - optimized with sharded locks and adaptive intervals.
        """
        consecutive_empty_polls = 0
        while not self._poll_event.is_set():
            try:
                cancel_q = []
                
                for shard_idx in range(self._shard_count):
                    reset_q_shard = []
                    with self._shard_locks[shard_idx]:
                        if self._dirty_shards[shard_idx] or consecutive_empty_polls == 0:
                            reset_q_shard = [q for q in self._active_q_shards[shard_idx] if not q.is_active()]
                            self._dirty_shards[shard_idx] = False
                    cancel_q.extend(reset_q_shard)
                
                if cancel_q:
                    consecutive_empty_polls = 0
                    for q in cancel_q:
                        print("Recycling non-consumed queue tickerId:", q._ticker_id)
                        self.cancel(q)
                else:
                    consecutive_empty_polls += 1
                    wait_time = min(self.p.poll_interval + consecutive_empty_polls * 0.05, 1.0) # adaptive
                    self._on_event.wait(timeout=wait_time)
            except Exception as e:
                print(f"Error in cycle worker: {e}")
                consecutive_empty_polls = 0
                self._poll_event.wait(timeout=1.0)  # 错误时等待更长时间

    def cancel(self, q):
        """
        Cancel data subscription by putting EOF into the queue and marking it for reuse.
        The queue will be available for reuse only after it's fully consumed.
        """
        # 使用分片锁移除队列
        self._remove_from_active_q(q)
        self._queue_pool.put(q)

    def dispose(self):
        # 清空所有分片（并行清理，减少阻塞时间）
        for shard_idx in range(self._shard_count):
            with self._shard_locks[shard_idx]:
                self._active_q_shards[shard_idx].clear()
                self._dirty_shards[shard_idx] = False
        self._queue_pool.clear()
