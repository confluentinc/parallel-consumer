#!/bin/bash
#
# Copyright (C) 2020-2023 Confluent, Inc.
#

set -xeu

pip install twine
# Expects username and password to be configured via environment variables
# See: https://twine.readthedocs.io/en/stable/#configuration
twine upload --repository-url https://upload.pypi.org/legacy/ ./dist/* --verbose --non-interactive