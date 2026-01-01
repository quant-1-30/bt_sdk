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


<!-- def factor(self, body: Query) -> List[Any]: # sid
    close = self.get_close(body)
    adjust = self.get_event("adjustment", body)
    right = self.get_event("rightment", body)
    from bt_sdk.core.helper.factor import calc_factor
    factors = calc_factor(close, adjust, right)
    return factors -->


``python
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

# no nagle on writer 
# server Delayed ACK / client `TCP_NODELAY`
# observer filter operate heavy cpu / resp --- bus to avoid filter

# cython not support lambda or nested
# reactivex subject --- on_next / on_complete / on_error 
            subject.pipe --- obseverable
            subscribe --- on_next / on_complete / on_error 

from reactivex.operators import as_iterable # blocking

for table in obs.pipe(as_iterable()):