# /usr/bin/env python3
# -*- coding: utf-8 -*-

import itertools
import threading
import collections
from queue import Queue

from bt_sdk.meta import MetaParams, with_metaclass
from bt_sdk.core.client.async_client import AsyncDatagramClient, AsyncStreamClient
from bt_sdk.utils.wrapper import retry_connection, singleton
from bt_sdk.utils.diagnosal import on_ping


class StatedQueue(Queue):
    """
    A wrapper around Queue that tracks consumption state.
    Maintains compatibility with standard Queue interface while adding state tracking.
    """
    def __init__(self):
        super().__init__()
        self._is_consumed = False
        self._ticker_id = None

    def is_consumed(self):
        """Mark this queue as consumed"""
        self._is_consumed = True

    def on_tracking(self, ticker_id):
        """Set tracking information"""
        print("on_tracking ", ticker_id)
        self._ticker_id = ticker_id

    def reset(self):
        """Reset queue state for reuse"""
        self._is_consumed = False
        self._ticker_id = None
        # Clear any remaining items
        while not self.empty():
            try:
                # nonblock method return data or raise error
                self.get_nowait()
            except:
                pass


class QueuePool:
    """
    A pool of StatedQueue objects for reuse.
    Pre-initializes a set of queues to avoid runtime creation overhead.
    When pool is empty, waits for a queue to become available instead of creating new ones.
    """
    def __init__(self, max_size=10):
        self._pool = []
        self._max_size = max_size
        # with Condition to acquire lock and  wait to release lock and wait for notify / condition has inner lock
        # Use Condition's lock for all synchronization
        self._available = threading.Condition()
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
                if not self._available.wait(timeout):
                    raise TimeoutError("Timeout waiting for available queue")
            q = self._pool.pop()
            q.reset()
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
                
            q.reset()
            self._pool.append(q)
            self._available.notify()

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


class MetaApi(MetaParams):
    
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
        _obj._lock_q = threading.Lock()
        _obj._active_q = set()
        _obj.qs = collections.OrderedDict()  # key: tickerId -> queue
        _obj.nextTickerId = itertools.count(0)
        # Initialize cycle event
        _obj._cycle_event = threading.Event()
        return _obj, args, kwargs
    
    def dopostinit(cls, _obj, *args, **kwargs):

        _obj, args, kwargs = super(MetaApi, cls).dopostinit(_obj, *args, **kwargs)
        
        # Initialize cycle thread
        _obj._cycle_thread = threading.Thread(
            target=_obj.on_cycle,
            daemon=True,
            name=f"cycle_worker_{id(_obj)}"
        )
        _obj._cycle_thread.start()
        return _obj, args, kwargs



class Api(with_metaclass(MetaApi, object)):

    params = (
        ("addr", ("127.0.0.1", 8888)),  # Default address tuple
        ("delay", "ms"),
        ("protocol", ""),
        ("queue_pool_size", 10),  # Add pool size parameter
        ("checksum", "eof"),
        ("cycle_interval", 1.0),  # Cycle check interval in seconds
    )
    
    def __enter__(self):
        return self
    
    def getTickQueue(self):
        '''Creates or reuses a Queue for data delivery to a data feed'''
        q = self._queue_pool.get()
        q.is_consumed()
        self._active_q.add(q)

        with self._lock_q:
            tickerId = next(self.nextTickerId)
            print("getTickQueue tickerId", tickerId)
            q.on_tracking(tickerId)
            self.qs[tickerId] = q
        return q
    
    def cancel(self, q):
        """
        Cancel data subscription by putting EOF into the queue and marking it for reuse.
        The queue will be available for reuse only after it's fully consumed.
        """
        # Return to pool if consumed
        with self._lock_q:
            q.reset()
            # del self.qs[q._ticker_id]
            self._queue_pool.put(q)
    
    @retry_connection(max_attempts=3, delay=1)
    def connected(self):
        """
            self.async_client.connected on ping
        """
        print("connected", self.p.addr[0], self.p.delay)
        return on_ping(self.p.addr[0], self.p.delay)
    
    def disconnected(self):
        """
            disconnect from the server
        """
        self.async_client.stop()
        # Stop the cycle thread
        self._cycle_event.set()
        if self._cycle_thread.is_alive():
            self._cycle_thread.join(timeout=1.0)
    
    def on_cycle(self):
        """
            on cycle
        """
        print("enter on_cycle", self._active_q)
        reset_q = [q for q in self._active_q if not q.is_consumed()]
        for q in reset_q:
            print("tickerId", q._ticker_id)
            self.cancel(q)
   
    def __exit__(self, exc_type, exc_value, traceback):
        
        self.async_client.stop()
        # Clear all queues
        with self._lock_q:
            self.qs.clear()
        # Clear the queue pool
        self._queue_pool.clear()
