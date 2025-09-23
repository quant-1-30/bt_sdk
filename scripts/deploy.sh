#! /bin/bash

# set +e # warning continue

CURRENT_DIR=$(pwd)
export PYTHONPATH=$CURRENT_DIR:$PYTHONPATH

# 检查 Poetry 是否安装
if ! command -v poetry &> /dev/null; then
    echo "Poetry is not installed. Installing Poetry..."
    curl -sSL https://install.python-poetry.org | python3 -
    export PATH="$HOME/.local/bin:$PATH"
fi

# 检查虚拟环境是否存在，如果不存在则创建
if [ ! -d "$(poetry env info --path 2>/dev/null)" ]; then
    echo "Creating Poetry virtual environment..."
    poetry install --no-root
fi

echo "Starting server in Poetry environment..."

poetry run devpi use http://192.168.2.100:3141/ 

# create user and channel for first time
# poetry run devpi user -c bt_sdk password=20210718 email=bt_sdk@example.com && poetry run devpi index -c bt_sdk/dev  bases=root/pypi && 

# use channel / login / upload
poetry run devpi use bt_sdk/dev 
poetry run devpi login bt_sdk --password 20210718 && rm -rf dist/ 
poetry run poetry build --format=wheel && poetry run devpi upload dist/*

