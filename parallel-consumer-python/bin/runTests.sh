#!/bin/bash

python setup.py bdist_wheel
pip install twine
twine upload -r pypi ./dist/* --verbose