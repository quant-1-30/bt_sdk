# bt_toolkit
toolkit for quoteApi and tradeApi
quoteApi --- grpc
tradeApi --- https 

#     # 网络传输base64编码替换 +与/ 特殊字符 返回bytes
#     # bytes ---> base64 encode(bytes-like ---> bytes-like object)
#     # base64 decode (str / bytes-like ---> bytes-like)
#     body = metadata.pop("body")
#     decode = {k: base64.b64decode(v) for k, v in body.items()}
#     decode = {k: json.loads(v.decode("utf-8")) for k, v in decode.items()}

# msgpack / struct 
# zlib --- stream data / gzip --- file

poetry build --format wheel (import path must be absolute path)

hub
devpi / pypiserver / bandersnatch

config devpi

# server 
poetry add devp-server devpi-web devpi-client

devpi-init --serverdir ~/.devpi 

devpi-server --serverdir ~/.devpi --host 0.0.0.0 --port 3141

# client
devpi use http://localhost:3141/
# default
devpi login root --password ''
# create bt_sdk
devpi user -c bt_sdk password=YOUR_SECRET email=bt_sdk@example.com
devpi login bt_sdk --password YOUR_SECRET
# create channel
devpi index -c bt_sdk/dev  bases=root/pypi
devpi use bt_sdk/dev
devpi use list ( devpi use --h)

# pip
# ~/.pip/pip.conf
[global]
index-url = http://localhost:3141/bt_sdk/dev/+simple/
trusted-host = localhost

# register the repo URL under the name “devpi”
# +simple/
poetry config repositories.devpi http://localhost:3141/bt_sdk/dev/+simple/

poetry config http-basic.devpi bt_sdk YOUR_SECRET_PASSWORD
