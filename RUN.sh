#!/bin/sh
python3 -m pip install --upgrade pip || python3 -m ensurepip
TMPDIR=/var/tmp python3 -m pip install -r requirements.txt
./generate.py -v --urls urls_meinung.txt dataset.csv "$@"
