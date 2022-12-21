#!/bin/bash
#
# Copyright (C) 2020-2022 Confluent, Inc.
#

set -x

python setup.py bdist_wheel
WHEEL_PATH=$(ls ./dist/*.whl)
pip install $WHEEL_PATH
pip install -r ./tests/requirements.txt
python3 -m pytest ./tests