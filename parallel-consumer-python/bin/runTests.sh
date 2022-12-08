#!/bin/bash

pip install .
pip install -r ./tests/requirements.txt
python -m pytest ./tests