#!/bin/bash
#
# Copyright (C) 2020-2022 Confluent, Inc.
#

set -x

pip install .
pip install -r ./tests/requirements.txt
python -m pytest ./tests