# bt_sdk Core 模块 Leak / Loop Binding 修复方案

## 问题总览

| # | 类别 | 严重度 | 问题 | 涉及文件 |
|---|------|--------|------|----------|
| 1 | Leak | 🔴 严重 | gRPC Channel 从不关闭 | `async_client.pyx` |
| 2 | Leak | 🔴 严重 | `_md_api_registry` 全局缓存永不清理 | `api.pyx` |
| 3 | Leak | 🔴 严重 | 进程退出时不清理 gRPC 连接 | `ctx.py` |
| 4 | Loop Binding | 🔴 严重 | `asyncio.Lock()` 绑定到错误 loop | `rpc/client.pyx` |
| 5 | Loop Binding | 🔴 严重 | `reset_connection()` 不重建 channel | `async_client.pyx` |
| 6 | Loop Binding | 🔴 严重 | `reset()` 导致 Stale Loop 引用 | `ctx.py` |

---

## 一、Leak 问题详解

### 问题 1：gRPC Channel 从不关闭

**根因**：`AsyncRpcClient` 继承自 `AsyncClient`，`close()` 调用 `_async_shutdown()`，但基类 `_async_shutdown()` 只处理 `listen_task`（在 `AsyncRpcClient` 中始终为 `None`），**从不关闭 gRPC channel**。

**修复前**（`bt_sdk/core/client/async_client.pyx`）：

```python
# 基类的 _async_shutdown —— AsyncRpcClient 未覆盖
async def _async_shutdown(self):
    if self.listen_task and not self.listen_task.done():  # listen_task 在 AsyncRpcClient 中始终为 None!
        self.listen_task.cancel()
        ...
```

**修复后**：

```python
# AsyncRpcClient 覆盖 _async_shutdown，关闭 gRPC channel
async def _async_shutdown(self):
    """覆盖基类：先取消 listen_task，再关闭 gRPC channel。"""
    await super()._async_shutdown()
    # 关闭 gRPC channel，防止资源泄漏
    try:
        await self.rpc_client.cleanup()
    except Exception as e:
        logger.warning(f"[{self.__class__.__name__}] gRPC cleanup error: {e}")
    self._connected = False
```

---

### 问题 2：`_md_api_registry` 全局缓存永不清理

**根因**：`GetMdApi()` 按 addr 缓存 `MdApi` 实例到模块级字典 `_md_api_registry`，但**没有任何代码从此字典中移除条目**。`disconnect()` / `__exit__()` 都不会清理它。

**修复前**（`bt_sdk/core/client/api.pyx`）：

```python
cdef dict _md_api_registry = {}

cpdef MdApi GetMdApi(tuple addr, int32_t timeout=30):
    # ... 仅往 _md_api_registry 写入，从不移除
```

**修复后**：

```python
cpdef void DisposeMdApi(tuple addr):
    """显式销毁并移除指定 addr 的 MdApi 实例，释放 gRPC channel 等资源。"""
    global _md_api_registry

    with _md_api_lock:
        instance = _md_api_registry.pop(addr, None)
        if instance is not None:
            try:
                instance.disconnect()
            except Exception as e:
                logger.warning(f"[MdApi] Dispose error for {addr}: {e}")


cpdef void dispose():
    """销毁并清空所有缓存的 MdApi 实例。通常在进程退出前调用。"""
    global _md_api_registry

    with _md_api_lock:
        addrs = list(_md_api_registry.keys())
    for addr in addrs:
        DisposeMdApi(addr)
```

---

### 问题 3：进程退出时不清理 gRPC 连接

**根因**：`cleanup_runner()` 使用 `AsyncRunner.reset()` 暴力销毁 loop，不关闭 gRPC channel。

**修复前**（`bt_sdk/ctx.py`）：

```python
def cleanup_runner():
    global _global_runner
    if _global_runner is not None:
        AsyncRunner.reset()   # 暴力销毁，不清理 gRPC
        _global_runner = None
```

**修复后**：

```python
def cleanup_runner():
    global _global_runner
    if _global_runner is not None:
        # 先清理所有缓存的 MdApi（gRPC channel），再停止 runner loop
        try:
            from bt_sdk.core.client.api import dispose
            dispose()
        except Exception as e:
            logger.warning(f"[SDK] dispose error: {e}")
        _global_runner.stop()  # stop() 标记 disposed
        _global_runner = None
```

---

## 二、Loop Binding 问题详解

### 问题 4：`asyncio.Lock()` 绑定到错误 loop

**根因**：`RpcClient.__init__()` 在同步上下文（无 running loop）中调用 `asyncio.Lock()`，导致 Lock 内部绑定到主线程默认 loop，后续在 runner loop 上使用时可能死锁或异常。

**修复前**（`bt_sdk/core/rpc/client.pyx`）：

```python
def __init__(self, str host="localhost", int port=50051):
    ...
    self._init_lock = asyncio.Lock()  # ← 无 running loop 时创建

async def initialize(self, ...):
    ...
    async with self._init_lock:  # ← 可能绑定到错误的 loop
```

**修复后**：

```python
def __init__(self, str host="localhost", int port=50051):
    ...
    self._init_lock = None  # 延迟到 initialize() 内创建

async def initialize(self, ...):
    ...
    # 延迟初始化 Lock，确保在正确的 event loop 内创建
    if self._init_lock is None:
        self._init_lock = asyncio.Lock()

    async with self._init_lock:
        ...
```

---

### 问题 5：`reset_connection()` 不重建 channel

**根因**：`reset_connection()` 仅设 `_connected = False`，不关闭旧 channel。下次 `_ensure_connection()` 时，`RpcClient.initialize()` 的 double-check（`if self._channel is not None: return`）会跳过重建，导致使用绑定在已关闭旧 loop 上的 channel。

**修复前**（`bt_sdk/core/client/async_client.pyx`）：

```python
cpdef void reset_connection(self):
    self._connected = False  # ← 只重置标志，旧 channel 仍在
```

**修复后**：

```python
cpdef void reset_connection(self):
    """关闭并清除旧 gRPC channel，使下次 _ensure_connection() 真正在新 loop 上重建。"""
    self._connected = False

    loop = self.loop
    if loop is not None and not loop.is_closed() and loop.is_running():
        try:
            fut = asyncio.run_coroutine_threadsafe(self.rpc_client.cleanup(), loop)
            fut.result(timeout=3)
        except Exception as e:
            logger.warning(f"[{self.__class__.__name__}] reset_connection cleanup error: {e}")
    else:
        # loop 不可用，同步置空避免跨 loop 使用
        self.rpc_client._channel = None
        self.rpc_client._stub = None
```

---

### 问题 6：`reset()` 导致 Stale Loop 引用（最关键）

**根因**：`initialize_runner()` 每次进入条件分支都调用 `AsyncRunner.reset()`，强制销毁单例并重建。但 `AsyncRunner.start()` 内部已处理 disposed→`_init()` 的场景，`reset()` 完全多余且有害。

**危害链条**：

```
reset() 关闭旧 Loop A
    → _md_api_registry 中缓存的 MdApi 仍持有 Loop A 引用
    → 新 Loop B 创建
    → GetMdApi 返回旧实例，旧 gRPC channel 绑定在已死的 Loop A 上
    → 崩溃
```

**修复前**（`bt_sdk/ctx.py`）：

```python
def initialize_runner():
    global _global_runner, _atexit_registered

    if _global_runner is None or not getattr(_global_runner, "_started", False):
        AsyncRunner.reset()  # ← 多余！强制销毁单例
        _global_runner = AsyncRunner()
        _global_runner.start()
        ...
```

**修复后**：

```python
def initialize_runner():
    global _global_runner, _atexit_registered

    if _global_runner is None:
        _global_runner = AsyncRunner()

    if not getattr(_global_runner, "_started", False):
        # start() 内部已处理 disposed → _init() 重建 loop/thread 的场景
        # 不需要 reset()，直接复用单例
        _global_runner.start()
        ...

    return _global_runner
```

**正确性保证**：

| 场景 | 行为 |
|------|------|
| runner 已启动 | `start()` 内部 `if self._started: return` 直接复用 |
| runner 存在但 disposed | `start()` 内部 `_init()` 重建 loop/thread |
| runner 为 None | 创建新实例，再 `start()` |

---

## 三、涉及文件清单

| 文件 | 修改内容 |
|------|----------|
| `bt_sdk/ctx.py` | `initialize_runner()` 移除 `reset()` 改复用；`cleanup_runner()` 改 `stop()` + `dispose()` |
| `bt_sdk/core/client/async_client.pyx` | `reset_connection()` 关闭旧 channel；新增 `_async_shutdown()` 覆盖调用 `rpc_client.cleanup()` |
| `bt_sdk/core/rpc/client.pyx` | `_init_lock` 延迟到 `initialize()` 内创建 |
| `bt_sdk/core/client/api.pyx` | 新增 `DisposeMdApi()` / `dispose()` 清理接口 |

---

## 四、验证方法

### 编译验证

```bash
python setup.py build_ext --inplace
```

### 运行时验证

```python
from bt_sdk.ctx import initialize_runner

# Test 1: 复用场景 — loop 不变
r1 = initialize_runner()
loop1 = r1.get_loop()
r2 = initialize_runner()
assert r1 is r2 and r1.get_loop() is loop1  # 复用

# Test 2: disposed 后重建 — 单例保持，loop 重建
r2.stop()
r3 = initialize_runner()
assert r1 is r3                            # 同一单例
assert r3.get_loop() is not loop1          # 新 loop