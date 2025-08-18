poetry build --format wheel (import path must be absolute path)

hub
devpi / pypiserver / bandersnatch

config devpi

# server 
poetry add devp-server devpi-web devpi-client

# devpi in-memory database 
devpi use --h # for help
devpi-init --serverdir ~/.devpi/server 

devpi-server --serverdir ~/.devpi --host 0.0.0.0 --port 3141

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