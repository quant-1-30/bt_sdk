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
