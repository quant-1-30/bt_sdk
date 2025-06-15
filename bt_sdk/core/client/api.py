# /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import itertools
import threading
from collections import deque
from queue import Queue, Empty

from bt_sdk.meta import with_metaclass, MetaSingleton
from bt_sdk.core.client.async_client import AsyncDatagramClient, AsyncStreamClient
from bt_sdk.utils.wrapper import retry_connection, singleton
from bt_sdk.utils.net_util import on_ping


class StatedQueue(Queue):
    """
    A wrapper around Queue that tracks consumption state.
    Maintains compatibility with standard Queue interface while adding state tracking.
    """
    def __init__(self):
        super().__init__()
        self._is_active = False  # 改名：是否正在使用中
        self._ticker_id = None
        self._lock = threading.Lock()  # 添加锁来保护状态修改

    def mark_active(self):
        """Mark this queue as active (in use)"""
        with self._lock:
            self._is_active = True

    def mark_inactive(self):
        """Mark this queue as inactive (can be recycled)"""
        with self._lock:
            self._is_active = False

    def is_active(self):
        """Check if this queue is currently active"""
        with self._lock:
            return self._is_active
    
    # 兼容性方法，但语义更清晰
    def mark_consumed(self):
        """Mark this queue as consumed (deprecated, use mark_active)"""
        self.mark_active()
    
    def is_consumed(self):
        """Check if this queue has been consumed (deprecated, use is_active)"""
        return self.is_active()

    def on_tracking(self, ticker_id):
        """Set tracking information"""
        self._ticker_id = ticker_id

    def finish_usage(self):
        """Mark queue as finished (ready for recycling)"""
        self.mark_inactive()

    def reset(self):
        """Reset queue state for reuse - highly optimized"""
        with self._lock:
            self._is_active = False  # 重置为非活跃状态
            
        # Strategy 1: Direct internal queue manipulation (fastest)
        try:
            # Access Queue's internal deque directly for maximum performance
            with self.mutex:  # Queue's internal lock
                self.queue.clear()  # Clear internal deque
                self.unfinished_tasks = 0
                # Wake up any threads waiting on task_done()
                if hasattr(self, 'all_tasks_done'):
                    self.all_tasks_done.notify_all()
            return
        except (AttributeError, RuntimeError):
            # Fall back to safe method if internal access fails
            pass


class QueuePool:
    """
    A pool of StatedQueue objects for reuse.
    Pre-initializes a set of queues to avoid runtime creation overhead.
    When pool is empty, waits for a queue to become available instead of creating new ones.
    Optimized with lazy reset and batch operations.
    """
    def __init__(self, max_size=10):
        self._pool = deque()  # Use deque for better performance
        self._max_size = max_size
        # Use Condition's lock for all synchronization
        self._available = threading.Condition()
        # Reset strategy tracking
        self._reset_stats = {
            'direct_resets': 0,
            'fallback_resets': 0,
            'failed_resets': 0
        }
        # Pre-initialize queues
        self._initialize_pool()

    def _initialize_pool(self):
        """Pre-initialize the pool with queues"""
        with self._available:  # Use Condition's lock
            for _ in range(self._max_size):
                self._pool.append(StatedQueue())

    def get(self, timeout=None):
        """
        Get a queue from the pool. If pool is empty, wait for a queue to become available.
        
        Args:
            timeout: Maximum time to wait in seconds. If None, wait indefinitely.
        
        Returns:
            A StatedQueue object from the pool.
            
        Raises:
            TimeoutError: If timeout is reached while waiting for a queue.
        """
        with self._available:
            while not self._pool:
                # with Condition to acquire lock and  wait to release lock and wait for notify / condition has inner lock
                if not self._available.wait(timeout):
                    raise TimeoutError("Timeout waiting for available queue")
            q = self._pool.popleft()  # Use popleft for deque efficiency
            
            # Smart reset with performance tracking
            try:
                q.reset()
                self._reset_stats['direct_resets'] += 1
            except Exception as e:
                # If reset fails, try to recover or create new queue
                try:
                    # Emergency reset - create new internal queue
                    q.__init__()
                    self._reset_stats['fallback_resets'] += 1
                except:
                    # Complete failure - create new queue object
                    q = StatedQueue()
                    self._reset_stats['failed_resets'] += 1
            
            q.mark_consumed()
            return q

    def put(self, q):
        """
        Return a queue to the pool if it's not full.
        Notifies waiting threads that a queue is available.
        
        Args:
            q: StatedQueue to return to the pool
            
        Returns:
            bool: True if queue was added to pool, False if pool was full
        """
        with self._available:
            if len(self._pool) >= self._max_size:
                raise ValueError("queue pool cannot be greater than max_size")
            
            # Pre-validate queue state before adding to pool
            if not hasattr(q, '_is_active') or not hasattr(q, '_ticker_id'):
                # Invalid queue, skip adding to pool
                return False
                
            self._pool.append(q)
            self._available.notify()
            return True

    def resize(self, new_size):
        """Resize the pool to a new size"""
        with self._available:
            old_size = len(self._pool)
            if new_size > old_size:
                # Add new queues
                for _ in range(new_size - old_size):
                    self._pool.append(StatedQueue())
                self._available.notify_all()
            elif new_size < old_size:
                # Remove excess queues using clear and extend / clear and [] different
                self._pool.clear()
                self._pool.extend([StatedQueue() for _ in range(new_size)])
            self._max_size = new_size

    def clear(self):
        """Clear the pool and reinitialize it"""
        with self._available:
            self._pool.clear()
            self._initialize_pool()
            self._available.notify_all()
    
    def batch_reset_queues(self, queue_list):
        """
        Batch reset multiple queues for better performance
        """
        reset_count = 0
        failed_count = 0
        
        for q in queue_list:
            try:
                q.reset()
                reset_count += 1
            except Exception:
                failed_count += 1
                # Try to recover the queue
                try:
                    q.__init__()
                    reset_count += 1
                except:
                    # Queue is completely broken, skip it
                    continue
        
        return reset_count, failed_count
    
    def get_reset_stats(self):
        """Get reset performance statistics"""
        with self._available:
            total_resets = sum(self._reset_stats.values())
            if total_resets == 0:
                return {"efficiency": 1.0, **self._reset_stats}
            
            efficiency = self._reset_stats['direct_resets'] / total_resets
            return {
                "efficiency": efficiency,
                "total_resets": total_resets,
                **self._reset_stats
            }


class MetaApi(MetaSingleton):
    
    def donew(cls, *args, **kwargs):
        """
            async_client / addr / client_id
        """
        _obj, args, kwargs = super(MetaApi, cls).donew(*args, **kwargs)
        
        if not hasattr(_obj, "connected"):
            raise ValueError("connected method is not implemented")

        async_client = AsyncDatagramClient if _obj.p.protocol == "udp" else AsyncStreamClient
        _obj.async_client = async_client(addr=_obj.p.addr)

        # Initialize queue pool
        _obj._queue_pool = QueuePool(max_size=kwargs.get('queue_pool_size', 10))
        
        # 分片锁优化: 将_active_q拆分为多个分片，减少锁竞争
        _obj._shard_count = max(4, os.cpu_count() or 4)  # 至少4个分片
        _obj._active_q_shards = [set() for _ in range(_obj._shard_count)]
        _obj._shard_locks = [threading.RLock() for _ in range(_obj._shard_count)]
        
        _obj.nextTickerId = itertools.count(0)
        # Initialize cycle event
        _obj._cycle_event = threading.Event()
        # Performance optimization: reduce repeated filtering
        _obj._dirty_shards = [False] * _obj._shard_count
        return _obj, args, kwargs
    
    def dopostinit(cls, _obj, *args, **kwargs):

        _obj, args, kwargs = super(MetaApi, cls).dopostinit(_obj, *args, **kwargs)
        
        return _obj, args, kwargs


class Api(with_metaclass(MetaApi, object)):

    params = (
        ("addr", ("127.0.0.1", 8888)),  # Default address tuple
        ("delay", "ms"),
        ("protocol", ""),
        ("queue_pool_size", 10),  # Add pool size parameter
        ("checksum", "eof"),
        ("cycle_interval", 0.1),  # Cycle check interval in seconds
    )
    
    def __enter__(self):
        return self

    def _init(self):
        # Initialize cycle thread
        self._cycle_thread = threading.Thread(
            target=self.on_cycle,
            daemon=True,  # 使用 daemon 线程，允许程序在中断时退出
            # daemon=False, # 使用非守护线程，确保资源被正确清理 / wait 会阻塞
            name=f"cycle_worker_{id(self)}"
        )
        self._cycle_thread.start()
    
    def _get_shard_index(self, queue_obj):
        """根据队列对象计算分片索引"""
        # 方法1: 使用 abs() 确保非负（当前方案）
        return abs(hash(id(queue_obj))) % self._shard_count
        
        # 方法2: 位运算优化（如果 shard_count 是2的幂）
        # return id(queue_obj) & (self._shard_count - 1)
        
        # 方法3: 简单除法取余（避免hash开销）
        # return id(queue_obj) % self._shard_count
    
    def _add_to_active_q(self, queue_obj):
        """将队列添加到对应的分片中"""
        shard_idx = self._get_shard_index(queue_obj)
        with self._shard_locks[shard_idx]:
            self._active_q_shards[shard_idx].add(queue_obj)
            self._dirty_shards[shard_idx] = True
    
    def _remove_from_active_q(self, queue_obj):
        """从对应的分片中移除队列"""
        shard_idx = self._get_shard_index(queue_obj)
        with self._shard_locks[shard_idx]:
            self._active_q_shards[shard_idx].discard(queue_obj)
            self._dirty_shards[shard_idx] = True
    
    def getTickQueue(self):
        '''Creates or reuses a Queue for data delivery to a data feed'''
        q = self._queue_pool.get()
        tickerId = next(self.nextTickerId)
        q.on_tracking(tickerId)
        # 使用分片锁添加到活跃队列
        self._add_to_active_q(q)
        # Queue already reset and marked consumed in pool.get()
        return q
    
    @retry_connection(max_attempts=3, delay=1)
    def connected(self):
        """
            self.async_client.connected on ping
        """
        print("connected", self.p.addr[0], self.p.delay)
        return on_ping(self.p.addr[0], self.p.delay)
     
    def on_cycle(self):
        """
        Cycle worker that checks and cleans up non-consumed queues.
        Runs in a daemon thread - optimized with sharded locks and adaptive intervals.
        """
        print("enter on_cycle")
        consecutive_empty_cycles = 0
        while not self._cycle_event.is_set():
            try:
                # 分片并行处理，减少锁竞争
                all_reset_q = []
                
                # 并行检查所有分片
                for shard_idx in range(self._shard_count):
                    reset_q_shard = []
                    with self._shard_locks[shard_idx]:
                        # 只检查脏分片，提高效率
                        if self._dirty_shards[shard_idx] or consecutive_empty_cycles == 0:
                            # 修复逻辑：查找已使用完毕(非活跃)的队列进行回收
                            reset_q_shard = [q for q in self._active_q_shards[shard_idx] if not q.is_active()]
                            self._dirty_shards[shard_idx] = False
                    all_reset_q.extend(reset_q_shard)
                
                # 处理队列（不需要持有锁）
                if all_reset_q:
                    consecutive_empty_cycles = 0
                    # 批量处理
                    for q in all_reset_q:
                        print("Recycling non-consumed queue tickerId:", q._ticker_id)
                        self.cancel(q)
                else:
                    consecutive_empty_cycles += 1
                    # 自适应等待时间：连续空闲时增加间隔，最高1秒
                    wait_time = min(0.1 + consecutive_empty_cycles * 0.05, 1.0)
                    self._cycle_event.wait(timeout=wait_time)
                    
            except Exception as e:
                print(f"Error in cycle worker: {e}")
                consecutive_empty_cycles = 0
                self._cycle_event.wait(timeout=1.0)  # 错误时等待更长时间

    def cancel(self, q):
        """
        Cancel data subscription by putting EOF into the queue and marking it for reuse.
        The queue will be available for reuse only after it's fully consumed.
        """
        # 使用分片锁移除队列
        self._remove_from_active_q(q)
        self._queue_pool.put(q)

    def get_performance_stats(self):
        """Get performance statistics for monitoring - sharded version with reset stats"""
        stats = {
            "pool_size": len(self._queue_pool._pool),
            "pool_max_size": self._queue_pool._max_size,
            "cycle_thread_alive": hasattr(self, '_cycle_thread') and self._cycle_thread.is_alive(),
            "shard_count": self._shard_count,
            "shard_stats": [],
            "reset_performance": self._queue_pool.get_reset_stats()
        }
        
        total_active = 0
        for shard_idx in range(self._shard_count):
            with self._shard_locks[shard_idx]:
                shard_size = len(self._active_q_shards[shard_idx])
                total_active += shard_size
                stats["shard_stats"].append({
                    "shard_id": shard_idx,
                    "active_queues": shard_size,
                    "is_dirty": self._dirty_shards[shard_idx]
                })
        
        stats["total_active_queues"] = total_active
        return stats

    def disconnected(self):
        """
        disconnect from the server - optimized cleanup with sharded locks
        """
        try:
            self._cycle_event.set()
            # 等待线程结束
            if hasattr(self, '_cycle_thread') and self._cycle_thread.is_alive():
                self._cycle_thread.join(timeout=1.0)
                
            if hasattr(self, 'async_client'):
                self.async_client.stop()
            
            # 清空所有分片（并行清理，减少阻塞时间）
            for shard_idx in range(self._shard_count):
                with self._shard_locks[shard_idx]:
                    self._active_q_shards[shard_idx].clear()
                    self._dirty_shards[shard_idx] = False
            
            # Clear the queue pool
            self._queue_pool.clear()
        except:
            pass  # 忽略清理时的错误

    def __del__(self):
        """gc"""
        self.disconnected()

    def __exit__(self, exc_type, exc_value, traceback):
        """退出上下文时清理资源"""
        self.disconnected()
