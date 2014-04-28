#!/bin/bash
set -e
set -x
python setup.py sdist
cp dist/*gz /n/ch2/var/www/sites/downloads/packages/pypi/drapps
python setup.py clean --all
