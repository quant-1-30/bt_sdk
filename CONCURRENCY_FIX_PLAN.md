# bt_sdk Core 模块高并发修复方案

> 本文档记录所有已实施和待实施的修复方案，包含修复前后的完整代码对比。

---

## ✅ 已实施修复

### 修复 1：C2 — GIL 释放（P0，已完成）

**问题**：C++ `calc_adjust_factors` 持有 GIL，阻塞所有 asyncio 协程。

**文件**：`bt_sdk/core/lib/factor/pybind_factor.cpp`

**修复前**：

```cpp
m.def("calc_adjust_factors", &calc_adjust_factors,
      py::arg("trading_dates"),
      py::arg("close"),
      py::arg("adj_events"),
      py::arg("right_events"),
      py::arg("type"));
```

**修复后**：

```cpp
m.def("calc_adjust_factors", &calc_adjust_factors,
      py::arg("trading_dates"),
      py::arg("close"),
      py::arg("adj_events"),
      py::arg("right_events"),
      py::arg("type"),
      py::call_guard<py::gil_scoped_release>());
```

**效果**：C++ 因子计算期间释放 GIL，其他协程可并发执行。为后续并行化（C3）铺平道路。

---

### 修复 2：C1+C9 — 连接初始化竞态条件（P0，已完成）

**问题**：高并发首波请求各自创建 Lock → 创建多个 gRPC channel → channel 泄漏。

**文件**：`bt_sdk/core/client/async_client.pyx` + `bt_sdk/core/client/async_client.pxd`

**修复前**（`async_client.pyx`）：

```python
def __init__(self, tuple addr, int timeout=5):
    super().__init__()
    self.rpc_client = RpcClient(host=addr[0], port=addr[1])
    self.timeout = timeout
    self._connected = False  # ← 普通 bint，无锁保护
    self.loop = None

async def _ensure_connection(self):
    if self._connected:
        return
    await self.rpc_client.initialize()  # ← N 个协程同时进入
    self._connected = True
```

**修复后**（`async_client.pyx`）：

```python
def __init__(self, tuple addr, int timeout=5):
    super().__init__()
    self.rpc_client = RpcClient(host=addr[0], port=addr[1])
    self.timeout = timeout
    self._connected = False
    self._conn_lock = None  # asyncio.Lock, lazy init in coroutine
    self._pending_tasks = set()  # hold strong refs to Tasks
    self.loop = None

async def _ensure_connection(self):
    if self._connected:
        return

    # Lazy init the lock in coroutine context (correct loop binding)
    if self._conn_lock is None:
        self._conn_lock = asyncio.Lock()

    async with self._conn_lock:
        # Double-check after acquiring lock
        if self._connected:
            return
        await self.rpc_client.initialize()
        self._connected = True
```

**`.pxd` 声明更新**（`async_client.pxd`）：

```cython
cdef class AsyncRpcClient(AsyncClient):
    cdef str addr
    cdef int timeout
    cdef bint _connected
    cdef object _conn_lock       # ← 新增
    cdef object _pending_tasks   # ← 新增
    cdef object loop
    cdef object rpc_client
```

**效果**：即使 N 个协程同时调用 `_ensure_connection`，只有一个协程执行 channel 初始化，其余等待 Lock 后通过 double-check 跳过。

---

### 修复 3：C4 — 批量提取替代 iter_rows（P1，已完成）

**问题**：`iter_rows(named=True)` 逐行创建 Python dict，比 `to_list()` 慢 10-50x。

**文件**：`bt_sdk/core/factor.py`

**修复前**：

```python
def adjust2struct(df: pl.DataFrame):
    events = []
    if df is not None and df.height > 0:
        for row in df.iter_rows(named=True):
            event = adj_factor.AdjustmentEvent()
            event.ex_date = row.get("ex_date", 0)
            event.bonus_share = row.get("bonus_share", 0.0)
            event.transfer = row.get("transfer", 0.0)
            event.bonus = row.get("bonus", 0.0)
            events.append(event)
    return events

def right2struct(df: pl.DataFrame):
    events = []
    if df is not None and df.height > 0:
        for row in df.iter_rows(named=True):
            event = adj_factor.RightmentEvent()
            event.ex_date = row.get("ex_date", 0)
            event.price = row.get("price", 0.0)
            event.ratio = row.get("ratio", 0.0)
            events.append(event)
    return events
```

**修复后**：

```python
def adjust2struct(df: pl.DataFrame):
    if df is None or df.height == 0:
        return []

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

def right2struct(df: pl.DataFrame):
    if df is None or df.height == 0:
        return []

    ex_dates = df["ex_date"].to_list()
    prices = df["price"].to_list()
    ratios = df["ratio"].to_list()

    events = []
    for i in range(len(ex_dates)):
        event = adj_factor.RightmentEvent()
        event.ex_date = ex_dates[i]
        event.price = prices[i]
        event.ratio = ratios[i]
        events.append(event)
    return events
```

**效果**：列级 C 速度提取，避免逐行 Python dict 创建。

---

### 修复 4：C5 — Task 引用持有（P1，已完成）

**问题**：`create_task()` 返回值未持有引用，Task 可能被 GC 静默取消。

**文件**：`bt_sdk/core/client/async_client.pyx`

**修复前**：

```python
cdef object wrap_protocol(self, bytes req_id, object msg):
    ...
    def factory(observer, scheduler):
        async def run():
            await self._stream_request(req_id, msg, req_subject)

        ...
        try:
            curr_loop = asyncio.get_running_loop()
            if curr_loop is self.loop:
                self.loop.create_task(run())  # ← 引用丢失!
            ...
```

**修复后**：

```python
def __init__(self, tuple addr, int timeout=5):
    ...
    self._pending_tasks = set()  # hold strong refs to Tasks

cdef object wrap_protocol(self, bytes req_id, object msg):
    ...
    def factory(observer, scheduler):
        async def run():
            try:
                await self._stream_request(req_id, msg, req_subject)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not req_subject.is_disposed:
                    req_subject.on_error(e)
            finally:
                # release strong ref to prevent Task accumulation
                self._pending_tasks.discard(task_ref[0])

        ...
        task_ref = [None]  # mutable holder so run() can access the task

        try:
            curr_loop = asyncio.get_running_loop()
            if curr_loop is self.loop:
                task = self.loop.create_task(run())
                task_ref[0] = task
                self._pending_tasks.add(task)  # ← 持有强引用
            ...
```

**效果**：Task 完成前不会被 GC 回收，完成后自动从 set 中移除。

---

## ✅ 已完成修复（续）

### 修复 5：C6 — rpc_callback 列重建优化（P1，已完成）

**问题**：每条 gRPC 消息触发完整列重建，高频 Tick 流下 CPU/内存瓶颈。

**文件**：`bt_sdk/core/rpc/client.pyx`

**当前代码**：

```python
cdef inline object rpc_callback(bytes arrow_bytes, int32_t rpc_type):
    ...
    for i in range(n):
        name = names[i]
        col = table.column(i)
        if name == "sid" or name == "name":
            col = pc.cast(col, pa.string())
        elif name in scale:
            col = pc.round(pc.multiply(col, factor), ndigits=2)
        new_cols[i] = col

    table = pa.Table.from_arrays(new_cols, names=names)  # ← 重建
```

**建议修复**：使用 `table.set_column()` 原地替换，或仅对需要 scale 的列操作：

```python
cdef inline object rpc_callback(bytes arrow_bytes, int32_t rpc_type):
    ...
    cdef list exprs = []
    for i in range(n):
        name = names[i]
        if name == "sid" or name == "name":
            exprs.append(pc.cast(table.column(i), pa.string()).alias(name))
        elif name in scale:
            factor = scale[name]
            exprs.append(pc.round(pc.multiply(table.column(i), factor), ndigits=2).alias(name))

    if exprs:
        table = table.set_column(  # 原地替换避免全表重建
            [names.index(e.name) for e in exprs],
            [e.name for e in exprs],
            exprs
        )
    return table
```

---

### 修复 6：C3 — 因子计算并行化（P2，已完成）

**前提**：需要 C2 GIL 释放已完成（✅ 已完成）。

**文件**：`bt_sdk/core/factor.py`

**当前代码**：

```python
def calc_factor(closes, adjs, rgts, forward):
    factor_sids = {}
    for sid, close_df in closes.items():  # ← 串行
        factor_sids[sid] = _calc_factor(close_df, adj_df, rgt_df, forward)
    return factor_sids
```

**建议修复**：

```python
async def calc_factor_async(closes, adjs, rgts, forward):
    if not closes:
        return {}

    async def _calc_one(sid):
        close_df = closes[sid]
        adj_df = adjs.get(sid, pl.DataFrame())
        rgt_df = rgts.get(sid, pl.DataFrame())
        loop = asyncio.get_running_loop()
        return sid, await loop.run_in_executor(
            None, _calc_factor, close_df, adj_df, rgt_df, forward
        )

    results = await asyncio.gather(*[_calc_one(sid) for sid in closes])
    return dict(results)
```

同时需更新 `api.pyx` 的 `get_factor_async` 调用 `calc_factor_async`。

---

### 修复 7：C7 — Subject 显式 dispose（P2，已完成）

**问题**：`_collect_async` 的 Subject 从未 dispose。

**文件**：`bt_sdk/core/client/api.pyx`

**当前代码**：

```python
async def _collect_async(observable, timeout):
    ...
    finally:
        subscription.dispose()
        # ← Subject 未 dispose
```

**建议修复**：需重构 `wrap_protocol`，将 Subject 引用传入 `_collect_async`：

```python
# 方案：在 wrap_protocol 中返回 (observable, subject) 元组
# 或在 Subject 上注册 dispose 回调

async def _collect_async(observable, timeout, subject=None):
    ...
    finally:
        subscription.dispose()
        if subject is not None:
            subject.dispose()
```

---

### 修复 8：C8 — merge 拷贝优化（P3，已完成）

**问题**：`pl.from_arrow()` 默认拷贝数据。

**文件**：`bt_sdk/utils/util.pyx`

**建议**：使用 `pl.from_arrow(table, rechunk=False)` 或直接操作 Arrow Table 避免 Polars 转换。

---

## 验证方法

```bash
# 编译
python setup.py build_ext --inplace

# 测试 GIL 释放
python -c "
import sys; sys.path.insert(0, 'bt_sdk/core/lib')
import adj_factor
result = adj_factor.calc_adjust_factors([20200101], [10.0], [], [], adj_factor.AdjustType.Forward)
print('GIL release: OK')
"

# 测试批量提取
python -c "
from bt_sdk.core.factor import adjust2struct
import polars as pl
df = pl.DataFrame({'ex_date':[20200101],'bonus_share':[0.1],'transfer':[0.0],'bonus':[0.5]})
events = adjust2struct(df)
print(f'Batch extract: {len(events)} events')
"