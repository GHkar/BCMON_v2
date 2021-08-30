#!/bin/bash

echo $(/bin/date '+%Y-%m-%d %H:%M:%S')
/usr/bin/python /collectData/log/log.py
/usr/bin/python /collectData/blocks/blocks.py
