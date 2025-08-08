
import threading
from queue import Queue, Empty
from collections import deque

__all__ = ["QueuePool"]


class PesudoQueue(Queue):
    """
    A wrapper around Queue that tracks consumption state.
    Maintains compatibility with standard Queue interface while adding state tracking.
    """
    def __init__(self, maxsize=10):
        super().__init__(maxsize)
        self._is_active = False  # 改名：是否正在使用中
        self._ticker_id = None
        self._lock = threading.Lock()  # 添加锁来保护状态修改

    def set_active(self, flag=True):
        """Mark this queue as active (in use)"""
        with self._lock:
            if flag:
                self._is_active = True 
            else:
                self._is_active = False

    def is_active(self):
        """Check if this queue is currently active"""
        with self._lock:
            return self._is_active
    
    def on_consumed(self):
        """Check if this queue has been consumed (deprecated, use is_active)"""
        self.set_active()

    def unique(self, ticker_id):
        """Set tracking information"""
        self._ticker_id = ticker_id

    def recycle(self):
        """Reset queue state for reuse - highly optimized"""
        self.set_active(False)
        try:
            # Access Queue's internal deque directly for maximum performance
            with self.mutex:  # Queue's internal lock
                self.queue.clear()  # Clear internal deque
                # Wake up any threads waiting on task_done()
                if hasattr(self, 'all_tasks_done'):
                    self.all_tasks_done.notify_all()
            return
        except (AttributeError, RuntimeError):
            pass


class QueuePool:
    """
    A pool of StatedQueue objects for reuse.
    Pre-initializes a set of queues to avoid runtime creation overhead.
    When pool is empty, waits for a queue to become available instead of creating new ones.
    Optimized with lazy reset and batch operations.
    """
    def __init__(self, pool_size=10):
        self._pool = deque()  # Use deque for better performance
        self.pool_size = pool_size
        # Use Condition's lock for all synchronization
        self._available = threading.Condition()
        # Reset strategy tracking
        # self._reset_stats = {
        #     'direct_resets': 0,
        #     'fallback_resets': 0,
        #     'failed_resets': 0
        # }
        # Pre-initialize queues
        self._initialize_pool()

    def _initialize_pool(self):
        """Pre-initialize the pool with queues"""
        with self._available:  # Use Condition's lock
            for _ in range(self.pool_size):
                self._pool.append(PesudoQueue())
    
    def resize(self, new_pool_size):
        """Resize the pool to a new size"""
        with self._available:
            old_size = len(self._pool)
            if new_pool_size > old_size:
                # Add new queues
                for _ in range(new_pool_size - old_size):
                    self._pool.append(PesudoQueue())
                self._available.notify_all()
            elif new_pool_size < old_size:
                # Remove excess queues using clear and extend / clear and [] different
                self._pool.clear()
                self._pool.extend([PesudoQueue() for _ in range(new_pool_size)])
            self.pool_size = new_pool_size

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
            
            try:
                q.recycle()
            except Exception as e:
                q = PesudoQueue()
            q.on_consumed()

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
            if len(self._pool) >= self.pool_size:
                raise ValueError("queue pool cannot be greater than max_size")
            
            # Pre-validate queue state before adding to pool
            if not hasattr(q, '_is_active') or not hasattr(q, '_ticker_id'):
                # Invalid queue, skip adding to pool
                return False
                
            self._pool.append(q)
            self._available.notify()
            return True
 
    def batch_recycle(self, queue_list):
        """
        Batch reset multiple queues for better performance
        """
        for q in queue_list:
            try:
                q.recycle()
            except Exception:
                # Queue is completely broken, skip it
                continue
        
    def clear(self):
        """Clear the pool and reinitialize it"""
        with self._available:
            self._pool.clear()
            self._initialize_pool()
            self._available.notify_all()
    