#!/bin/bash

echo $(/bin/date '+%Y-%m-%d %H:%M:%S')
/usr/bin/python /collectData/log/log.py
/bin/sed -i "1s/.*/0/g" /collectData/log/nowPos.txt  # init pos 0
/bin/cat /dev/null > /root/.bitcoin/debug.log	# debug.log file clear
