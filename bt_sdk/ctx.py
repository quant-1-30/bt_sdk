import os
import atexit
import contextlib

from bt_sdk.utils.runner import AsyncRunner

_global_runner = None

def initialize_runner():
    global _global_runner

    if _global_runner is None:
        _global_runner = AsyncRunner()
        _global_runner.start()
        print(f"[MdProvider] Global AsyncRunner started. Loop: {id(_global_runner.get_loop())}")
        # execute when exit Python Airflow Task/Ray Worker
        atexit.register(cleanup_runner)
    return _global_runner


def cleanup_runner():
    global _global_runner
    if _global_runner:
        print("[SDK] Process exiting, cleaning up AsyncRunner...")
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
        # reuse global runner for next dag task
        pass
