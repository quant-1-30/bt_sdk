poetry build --format wheel (import path must be absolute path)

hub
devpi / pypiserver / bandersnatch

config devpi

# server 
poetry add devp-server devpi-web devpi-client

# devpi in-memory database 
devpi use --h # for help
devpi-init --serverdir ~/.devpi/server 

devpi-server --serverdir ~/.devpi --host 192.168.2.100 --port 3141

# client
devpi use http://localhost:3141/
# default
devpi login root --password ''

# create bt_sdk
devpi user -c bt_sdk password=20210718 email=bt_sdk@example.com
devpi login bt_sdk --password 20210718

# create channel
devpi index -c bt_sdk/dev  bases=root/pypi
devpi use bt_sdk/dev 

# upload
devpi upload dist/*

# list
devpi list bt_sdk

# pip
# ~/.pip/pip.conf
[global]
index-url = http://localhost:3141/bt_sdk/dev/+simple/
trusted-host = localhost

# register the repo URL under the name “devpi”
# +simple/
poetry config repositories.devpi http://localhost:3141/bt_sdk/dev/+simple/

poetry config http-basic.devpi bt_sdk YOUR_SECRET_PASSWORD

poetry cache clear pypi --all

poetry env info --path

pytest 中执行多个测试用例时，每个测试用例都会创建新的 Api 实例

# poetry 不会自动打包pybind  需要手动cmake构建 / pyproject.toml 配置 include

# rxpy 响应性编程重构
# cython 重构 api and client 避免asyncio.queue

# no nagle on writer 
# server Delayed ACK / client `TCP_NODELAY`
# observer filter operate heavy cpu / resp --- bus to avoid filter

# cython not support lambda or nested
# reactivex subject --- on_next / on_complete / on_error 
            subject.pipe --- obseverable
            subscribe --- on_next / on_complete / on_error 

a. async for x in observable.to_async_iterable()

``python
from reactivex.operators import as_iterable # blocking
b.for table in obs.pipe(as_iterable()):
c.  obs.subscribe(
        on_next=q.put,
        on_completed=lambda: q.put(None) # 发送结束信号
    )

import pyarrow as pa

def bytes_to_table(data: bytes):
    # 1. 将 bytes 包装成 BufferReader (Zero-copy)
    reader = pa.BufferReader(data)
    
    # 2. 打开 IPC 流读取器
    # open_stream 专门用于解析由 new_stream 生成的数据
    try:
        with pa.ipc.open_stream(reader) as stream_reader:
            # 3. 读取所有 Batch 并合并回 Table
            table = stream_reader.read_all()
            return table
    except pa.ArrowInvalid:
        # 如果数据为空或格式不正确
        return None
```

# .pxd 函数体**：Cython 要求必须inline (pure C)
* .pyx 函数体且没有 pxd` private

# zmq / tcp 长链接问题 
a. cache writer and reader for reuse  not close api, tcp established tcp status
b. writer --- fd
c. server fin ---> client readexactly incompleteReadError
d. long reuse key is to avoid open_connection
e. usage pattern --- singelton long live in process / reference not gc
f. close and wait_closed --- writer.close() nonblock  send fin and stop writer / wait_closed to flush blocking

**避免阻塞**：`wait_closed()` 可能会因为网络状况差、对方没有 ACK 而卡住几百毫秒甚至数秒。如果在 `finally` 块中 `await` 它，你的重连逻辑（`await asyncio.sleep(3)`）就会被顺延推迟，导致系统恢复变慢

**系统资源处理**：当你执行 `self._writer = None` 并且旧的 `writer` 对象不再被引用时，Python 会自动清理。旧连接在操作系统层面会进入 `FIN_WAIT` 状态，由操作系统内核自行处理，不会占用你 Python 进程的逻辑资源

**原因**：如果你在 `close()` 之后立即退出 Python 解释器或停止事件循环（Event Loop），而没有 `wait_closed()`，那么缓冲区中还没来得及发出的数据（比如最后一条撤单指令）可能会被**强行丢弃**

MANIFEST.in` 决定文件会被包含在 **源码分发包 (sdist, 即 .tar.gz 文件)**

python -m build --wheel --no-isolation # setuptool 
poetry build --format wheel # pure python


*结论：必须改为 `async def` 并使用 `await`。绝对不能使用 `fut.result()`。**

在 Ray Async Actor（以及任何单线程 Asyncio 环境）中，调用 `future.result()` 会导致 **即刻死锁**。

### 1. 为什么 `fut.result()` 会死锁？

*   **场景**：你的 `TdApi` 底层依附于 Ray Actor 的主 Event Loop。
*   **机制**：网络数据的接收（`socket.recv`）和回调的处理（`set_result`）都需要这个 Loop 来驱动。
*   **死锁流程**：
    1.  你调用 `fut.result()`。
    2.  主线程被**阻塞**，停止运行 Event Loop，死等 Future 完成。
    3.  因为 Loop 停了，底层的 `AsyncStreamClient` 无法读取网络包，也就无法设置 Future 的结果。
    4.  **结果**：主线程在等 Future，Future 在等主线程（Loop）干活。永久卡死

# cython pxd ? ---> default 不能跳开
