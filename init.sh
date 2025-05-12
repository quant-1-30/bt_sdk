#! /bin/bash

# set +e # warning continue

# initialize database
poetry run python init.py

# activte web
cd crm_backend && poetry run python main.py
