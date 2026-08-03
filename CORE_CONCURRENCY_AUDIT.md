# bt_sdk Core 模块高并发性能审查报告

## 审查范围

本报告针对 `bt_sdk/core` 模块在高并发场景下的 **资源泄漏（Leak）** 和 **性能瓶颈（Performance）** 进行深度审查。

涉及文件：
- `bt_sdk/core/client/api.pyx` — MdApi 对外接口层
- `bt_sdk/core/client/async_client.pyx` — 异步客户端、Subject/Observable 管理
- `bt_sdk/core/rpc/client.pyx` — gRPC 底层通信、Arrow 反序列化
- `bt_sdk/core/factor.py` — 因子计算调度层
- `bt_sdk/core/lib/factor/` — C++ 因子计算引擎 + pybind11 绑定
- `bt_sdk/utils/runner.py` — 全局事件循环单例
- `bt_sdk/utils/util.pyx` — DataFrame 合并工具

---

## 一、问题总览

| # | 类别 | 严重度 | 问题 | 涉及文件 |
|---|------|--------|------|----------|
| C1 | 并发瓶颈 | 🔴 严重 | `_ensure_connection` 的 `_init_lock` 延迟初始化存在竞态条件 | `async_client.pyx` |
| C2 | 性能瓶颈 | 🔴 严重 | C++ `calc_adjust_factors` 持有 GIL，阻塞所有协程 | `pybind_factor.cpp` |
| C3 | 性能瓶颈 | 🔴 严重 | `calc_factor()` 串行遍历所有 sid，无并行化 | `factor.py` |
| C4 | 性能瓶颈 | 🟡 中等 | `adjust2struct`/`right2struct` 使用 `iter_rows` 逐行转换 | `factor.py` |
| C5 | Leak | 🟡 中等 | `wrap_protocol` 中 `create_task` 返回值未持有引用 | `async_client.pyx` |
| C6 | 性能瓶颈 | 🟡 中等 | `rpc_callback` 逐列重建 Table，热路径内存分配过多 | `rpc/client.pyx` |
| C7 | Leak | 🟡 中等 | `_collect_async` 的 `Subject` 未显式 `dispose` | `api.pyx` |
| C8 | 性能瓶颈 | 🟢 低 | `_merge2DataFrame` 多次 `pl.from_arrow` 拷贝 | `util.pyx` |
| C9 | 并发风险 | 🟡 中等 | `RpcClient.initialize` 的 `_init_lock is None` 检查无原子保护 | `rpc/client.pyx` |

---

## 二、并发瓶颈详解

### C1 + C9：`_ensure_connection` 与 `_init_lock` 的双重竞态条件

**根因**：`RpcClient.initialize()` 中 `_init_lock` 的延迟初始化不是原子的：

```python
# rpc/client.pyx
async def initialize(self, ...):
    if self._channel is not None:
        return

    # 竞态窗口：两个协程同时到达这里
    if self._init_lock is None:        # ← 两个协程都看到 None
        self._init_lock = asyncio.Lock()  # ← 创建两个不同的 Lock!

    async with self._init_lock:        # ← 各自用不同的 Lock，double-check 失效
        if self._channel is not None:
            return
        ...  # 两个协程都会进入这里，创建两个 channel!
```

**高并发场景**：当 N 个 `get_factor_async` 同时调用时，首批请求全部看到 `_init_lock is None`，各自创建 Lock 并进入临界区，导致**多个 gRPC channel 被创建**。`AsyncRpcClient._ensure_connection` 也有类似问题——`_connected` 是普通 `bint`，无锁保护。

**影响**：资源浪费 + gRPC channel 泄漏 + 连接数暴涨。

---

### C2：C++ 因子计算持有 GIL

**根因**：`pybind_factor.cpp` 的 `calc_adjust_factors` 绑定未声明 `py::call_guard<py::gil_scoped_release>()`：

```cpp
// pybind_factor.cpp (当前)
m.def("calc_adjust_factors", &calc_adjust_factors, ...);
// 缺少 py::call_guard<py::gil_scoped_release>()
```

**影响**：C++ 计算期间 GIL 不释放，所有 asyncio 协程被阻塞。如果 N 个策略同时请求因子计算，它们**串行执行**，完全无法利用 asyncio 的并发优势。

---

### C3：`calc_factor()` 串行遍历

**根因**：

```python
# factor.py
def calc_factor(closes, adjs, rgts, forward):
    factor_sids = {}
    for sid, close_df in closes.items():  # ← 串行遍历每个 sid
        factor_sids[sid] = _calc_factor(close_df, adj_df, rgt_df, forward)
    return factor_sids
```

**影响**：假设 3000 只股票，每只 `_calc_factor` 耗时 ~1ms（含 C++ 调用 + Python 转换），总计 3s。此期间 GIL 被 C++ 持有（C2），整个 event loop 冻结。

---

### C4：`iter_rows` 逐行 Python 转换

**根因**：

```python
# factor.py
def adjust2struct(df):
    events = []
    for row in df.iter_rows(named=True):  # ← 逐行 Python dict 创建
        event = adj_factor.AdjustmentEvent()
        event.ex_date = row.get("ex_date", 0)
        event.bonus_share = row.get("bonus_share", 0.0)
        ...
```

**影响**：`iter_rows(named=True)` 返回 Python dict，逐行创建 pybind11 对象。相比 `df["ex_date"].to_list()` 批量提取，慢 10-50x。

---

## 三、Leak 详解

### C5：`create_task` 返回值未持有引用

**根因**：

```python
# async_client.pyx wrap_protocol factory()
if curr_loop is self.loop:
    self.loop.create_task(run())  # ← Task 引用丢失
```

**影响**：CPython 的 asyncio 实现中，Task 的弱引用可能被 GC 回收，导致协程被静默取消。官方文档明确警告："Save a reference to the result of this function, to avoid a task disappearing mid-execution."

---

### C7：`Subject` 未显式 dispose

**根因**：`_collect_async` 的 `finally` 只 dispose 了 `subscription`，但 `Subject`（在 `wrap_protocol` 中创建）从未被 `dispose()`：

```python
# api.pyx _collect_async
finally:
    subscription.dispose()  # ← 只 dispose subscription
    # Subject 内部的 observer 链可能泄漏
```

**影响**：Subject 持有 observer 回调引用，如果 `on_next`/`on_error` 的闭包捕获了大对象（如 `buffer` list），这些对象不会被 GC。

---

## 四、性能瓶颈详解

### C6：`rpc_callback` 逐列重建 Table

**根因**：

```python
# rpc/client.pyx
cdef inline object rpc_callback(bytes arrow_bytes, int32_t rpc_type):
    ...
    for i in range(n):
        name = names[i]
        col = table.column(i)
        if name == "sid" or name == "name":
            col = pc.cast(col, pa.string())     # ← 新数组
        elif name in scale:
            col = pc.round(pc.multiply(col, factor), ndigits=2)  # ← 2个中间数组
        new_cols[i] = col

    table = pa.Table.from_arrays(new_cols, names=names)  # ← 重建整个 Table
```

**影响**：每条 gRPC 响应消息都触发完整的列重建。对于高频 Tick 流（每秒数百条），这是严重的 CPU + 内存分配瓶颈。`pc.multiply` + `pc.round` 各创建一个中间 ChunkedArray。

---

### C8：`_merge2DataFrame` 多次拷贝

**根因**：

```python
# util.pyx
for sid_byte, bulk_batch in sid_to_batches.items():
    aligned_table = pa.concat_tables(bulk_batch, promote_options='default')
    aligned[sid_byte] = pl.from_arrow(aligned_table)  # ← Arrow → Polars 拷贝
```

**影响**：`pl.from_arrow()` 默认会拷贝数据（除非数据已是 compatible 格式）。N 个 sid = N 次拷贝。

---

## 五、修复方案

### 方案 1：修复 `_init_lock` 竞态条件（C1 + C9）

**文件**：`bt_sdk/core/rpc/client.pyx`

```python
def __init__(self, str host="localhost", int port=50051):
    ...
    self._init_lock = None
    self._init_lock_creation = asyncio.Lock()  # ← 保护 Lock 本身的创建

async def initialize(self, ...):
    if self._channel is not None:
        return

    if self._init_lock is None:
        async with self._init_lock_creation:  # ← 原子化 Lock 创建
            if self._init_lock is None:       # ← double-check
                self._init_lock = asyncio.Lock()

    async with self._init_lock:
        if self._channel is not None:
            return
        ...  # 创建 channel
```

同样需要为 `AsyncRpcClient._connected` 添加 `asyncio.Lock` 或使用 `asyncio.Event`。

---

### 方案 2：释放 GIL（C2）

**文件**：`bt_sdk/core/lib/factor/pybind_factor.cpp`

```cpp
m.def("calc_adjust_factors", &calc_adjust_factors,
      py::arg("trading_dates"),
      py::arg("close"),
      py::arg("adj_events"),
      py::arg("right_events"),
      py::arg("type"),
      py::call_guard<py::gil_scoped_release>());  // ← 添加这行
```

C++ `calc_adjust_factors` 纯计算，不回调 Python，释放 GIL 是安全的。

---

### 方案 3：并行化因子计算（C3 + C2）

**文件**：`bt_sdk/core/factor.py`

将 `calc_factor` 改为异步，使用 `asyncio.gather` 并行调度（配合 C2 的 GIL 释放）：

```python
async def calc_factor_async(closes, adjs, rgts, forward):
    if not closes:
        return {}

    async def _calc_one(sid):
        close_df = closes[sid]
        adj_df = adjs.get(sid, pl.DataFrame())
        rgt_df = rgts.get(sid, pl.DataFrame())
        # run_in_executor 让 C++ 计算在线程池中并行
        loop = asyncio.get_running_loop()
        return sid, await loop.run_in_executor(
            None, _calc_factor, close_df, adj_df, rgt_df, forward
        )

    results = await asyncio.gather(*[_calc_one(sid) for sid in closes])
    return dict(results)
```

> **注意**：需要配合 `py::call_guard<py::gil_scoped_release>()` 才能真正并行。

---

### 方案 4：批量提取替代 `iter_rows`（C4）

**文件**：`bt_sdk/core/factor.py`

```python
def adjust2struct(df):
    if df is None or df.height == 0:
        return []

    # 批量提取为 Python list（C 级速度）
    ex_dates = df["ex_date"].to_list()
    bonus_shares = df["bonus_share"].to_list()
    transfers = df["transfer"].to_list()
    bonuses = df["bonus"].to_list()

    events = []
    for i in range(len(ex_dates)):
        event = adj_factor.AdjustmentEvent()
        event.ex_date = ex_dates[i]
        event.bonus_share = bonus_shares[i]
        event.transfer = transfers[i]
        event.bonus = bonuses[i]
        events.append(event)
    return events
```

---

### 方案 5：持有 Task 引用（C5）

**文件**：`bt_sdk/core/client/async_client.pyx`

```python
cdef class AsyncRpcClient(AsyncClient):
    def __init__(self, tuple addr, int timeout=5):
        ...
        self._pending_tasks = set()  # ← 持有 Task 强引用

    cdef object wrap_protocol(self, bytes req_id, object msg):
        ...
        def factory(observer, scheduler):
            async def run():
                try:
                    await self._stream_request(req_id, msg, req_subject)
                finally:
                    self._pending_tasks.discard(task)  # ← 完成后移除

            ...
            task = self.loop.create_task(run())
            self._pending_tasks.add(task)  # ← 持有引用
```

---

### 方案 6：Subject 显式 dispose（C7）

**文件**：`bt_sdk/core/client/api.pyx`

```python
async def _collect_async(observable, timeout):
    ...
    cdef object req_subject = None  # 需要从 observable 获取或在外部传入
    try:
        ...
    finally:
        subscription.dispose()
        if req_subject is not None:
            req_subject.dispose()  # ← 显式释放
```

> **注意**：当前架构中 `Subject` 在 `wrap_protocol` 内创建，`_collect_async` 无法访问。需要重构传参或在 `Subject` 上注册 `on_dispose` 回调。

---

## 六、优先级排序与修复状态

| 优先级 | 问题 | 修复难度 | 预期收益 | 状态 |
|--------|------|----------|----------|------|
| P0 | C2 GIL 释放 | 低（1 行代码） | 🔴 极高 | ✅ 已修复 |
| P0 | C1+C9 `_init_lock` 竞态 | 中 | 🔴 高 | ✅ 已修复 |
| P1 | C4 `iter_rows` → 批量提取 | 低 | 🟡 高 | ✅ 已修复 |
| P1 | C5 Task 引用 | 低 | 🟡 中 | ✅ 已修复 |
| P1 | C6 rpc_callback 列重建优化 | 中 | 🟡 中 | ⏳ 待实施 |
| P2 | C3 因子并行化 | 高 | 🟡 高（需配合 C2） | ⏳ 待实施 |
| P2 | C7 Subject dispose | 中 | 🟢 中 | ⏳ 待实施 |
| P3 | C8 merge 拷贝优化 | 中 | 🟢 低 | ⏳ 待实施 |

---

## 七、已实施的修复摘要

### ✅ P0-C2: GIL 释放（`pybind_factor.cpp`）
添加 `py::call_guard<py::gil_scoped_release>()`，C++ 因子计算不再阻塞 event loop。

### ✅ P0-C1+C9: 连接初始化竞态修复（`async_client.pyx`）
- 新增 `self._conn_lock = asyncio.Lock()` 在 `_ensure_connection()` 中
- 使用 double-check + Lock 保护 channel 初始化
- 防止高并发首波请求创建多个 gRPC channel

### ✅ P1-C4: 批量提取优化（`factor.py`）
- `adjust2struct` 和 `right2struct` 从 `iter_rows(named=True)` 改为 `to_list()` 批量提取
- 预期 10-50x 加速

### ✅ P1-C5: Task 引用持有（`async_client.pyx`）
- 新增 `self._pending_tasks = set()`
- `create_task()` 返回的 Task 存入 set 防止 GC
- `finally` 块中 `discard` 释放已完成 Task
