import pytest
import threading
import time
from bt_sdk.core.client.api import Api

# 存储所有创建的 Api 实例
_api_instances = []

def cleanup_api(api):
    """Cleanup a single API instance"""
    if hasattr(api, 'disconnected'):
        api.disconnected()
        # 等待线程结束
        if hasattr(api, '_cycle_thread') and api._cycle_thread.is_alive():
            api._cycle_thread.join(timeout=1.0)

@pytest.fixture(autouse=True)
def api_cleanup():
    """Fixture to track Api instances"""
    yield
    # 每个测试结束后清理
    for api in _api_instances:
        cleanup_api(api)

def pytest_sessionfinish(session, exitstatus):
    """在所有测试完成后执行清理"""
    print("\nCleaning up after all tests...")
    for api in _api_instances:
        cleanup_api(api)
    _api_instances.clear()

def pytest_configure(config):
    """在测试开始前设置"""
    # 设置线程清理超时
    threading._shutdown_timeout = 1.0

@pytest.fixture
def api():
    """Create an Api instance for testing"""
    api = Api()
    _api_instances.append(api)
    return api 