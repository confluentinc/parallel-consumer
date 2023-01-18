#!/bin/bash
#
# Copyright (C) 2020-2023 Confluent, Inc.
#

set -xeu

SKIP_TESTS=${1:-"false"}
if [ "$SKIP_TESTS" = "true" ]
then
    echo 'Skipping tests'
    exit 0;
fi

python3 setup.py bdist_wheel
WHEEL_PATH=$(ls ./dist/*.whl)
pip install $WHEEL_PATH
pip install -r ./tests/requirements.txt
python3 -m pytest ./tests