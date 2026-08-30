#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import threading
import logging
import errno

logger = logging.getLogger(__name__)


class AsyncRunner:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None or cls._instance._disposed:
                cls._instance = super().__new__(cls)
                cls._instance._init()
            return cls._instance

    def _init(self):
        self._loop = asyncio.new_event_loop()
        self._thread = None
        self._ready_event = threading.Event()
        self._started = False
        self._disposed = False

    def start(self):
        with AsyncRunner._lock:
            if self._started:
                return
            if self._disposed:
                # recreate loop/thread after dispose
                self._init()

            self._thread = threading.Thread(target=self._run_loop, name="GlobalAsyncLoop", daemon=True)
            self._thread.start()
            self._ready_event.wait()
            self._started = True
            logger.info(f"Global AsyncRunner started. Loop: {id(self._loop)}")

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)

        self._loop.set_exception_handler(self._ignore_errno35)  # macos gRPC Error

        self._ready_event.set()
        try:
            self._loop.run_forever()
        finally:
            # tolerate double close: _stop_impl may close the loop from another
            # thread while we are between run_forever() returning and close()
            try:
                self._loop.close()
            except RuntimeError:
                pass

    def _ignore_errno35(self, loop, context):
        exc = context.get('exception')
        if isinstance(exc, BlockingIOError) and exc.errno == errno.EAGAIN:
            return
        if "Resource temporarily unavailable" in context.get("message", ""):
            return
        loop.default_exception_handler(context)

    def get_loop(self):
        return self._loop

    def run_sync(self, coro):
        if not self._started:
            self.start()

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return future.result()
        except Exception as e:
            logger.error(f"Async call failed: {e}")
            raise e

    def _stop_impl(self):
        """Internal stop logic without acquiring the class lock (avoid deadlock)."""
        if not self._started:
            return

        loop = self._loop
        thread = self._thread
        self._started = False
        self._disposed = True

        if loop is not None and not loop.is_closed() and loop.is_running():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass  # loop closed concurrently

        if thread is not None and thread.is_alive():
            thread.join(timeout=5)
            if thread.is_alive():
                # Python cannot force-kill a thread. Surface the leak loudly
                # (zombie loop thread + pending tasks) instead of silently
                # replacing it; a fresh runner will be built on next use
                # because _disposed is already set.
                logger.warning(
                    "GlobalAsyncRunner loop thread did not exit within 5s "
                    "(pending gRPC/asyncio tasks?). Thread leaked; a fresh "
                    "runner/loop will be created on next use."
                )
            else:
                logger.info("Global AsyncRunner stopped.")

        # Loop never ran (thread died early) or already stopped but not yet
        # closed by the thread's finally: release selector/fds from this side.
        # The thread's finally-close tolerates the double close.
        if loop is not None and not loop.is_closed() and not loop.is_running():
            try:
                loop.close()
            except RuntimeError:
                pass

    def stop(self):
        """Stop the runner and mark it as disposed so a new instance can be created."""
        with AsyncRunner._lock:
            self._stop_impl()

    @classmethod
    def reset(cls):
        """Force-clear the singleton so the next AsyncRunner() builds a fresh one."""
        with cls._lock:
            inst = cls._instance
            if inst is not None:
                inst._stop_impl()
            cls._instance = None
