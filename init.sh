#! /bin/bash

# set +e # warning continue

# activte web
poetry build --format wheel
# upload to devpi
