import os
import atexit
import contextlib
import logging

from bt_sdk.utils.runner import AsyncRunner

logger = logging.getLogger(__name__)

_global_runner = None
_atexit_registered = False


def initialize_runner():
    global _global_runner, _atexit_registered

    if _global_runner is None:
        _global_runner = AsyncRunner()

    if not getattr(_global_runner, "_started", False):
        _global_runner.start()
        logger.info(f"[MdProvider] Global AsyncRunner started. Loop: {id(_global_runner.get_loop())}")

    if not _atexit_registered:
        atexit.register(cleanup_runner)
        _atexit_registered = True

    return _global_runner


def cleanup_runner():
    global _global_runner
    if _global_runner is not None:
        logger.info("[SDK] Process exiting, cleaning up AsyncRunner...")
        try:
            from bt_sdk.core.client.api import dispose
            dispose()
        except Exception as e:
            logger.warning(f"[SDK] dispose error: {e}")
        finally:
            _global_runner.stop()  
            _global_runner = None


def get_md_api(addr_str=None, timeout=30):
    from bt_sdk.core.client.api import GetMdApi

    if not addr_str:
        addr_str = os.getenv("MD_ADDR", "127.0.0.1:50051")
    ip, port = addr_str.split(":")
    return GetMdApi(addr=(ip, int(port)), timeout=timeout)


@contextlib.contextmanager
def external_mdapi_context(addr_str=None, timeout=30):
    runner = initialize_runner()

    mdapi = get_md_api(addr_str, timeout)
    mdapi.start(runner.get_loop())

    try:
        yield mdapi
    finally:
        # runner and mdapi reuse global instances, context exit does not close connections
        # gRPC channel and event loop are cleaned up uniformly at process exit by cleanup_runner / atexit
        pass
