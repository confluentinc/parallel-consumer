#!/bin/bash
#
# Copyright (C) 2020-2022 Confluent, Inc.
#


python setup.py bdist_wheel
pip install twine
twine upload -r pypi ./dist/* --verbose