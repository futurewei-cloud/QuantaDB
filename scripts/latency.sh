#!/bin/bash

if [ $# -ne 1 ]; then
	echo "usage: latency.sh <dssn-log-dir>"
	exit 1
fi
dir=$1
cnt=$(ls $dir/server*log | wc -l)
for i in $(seq 1 $cnt); do
	rm /tmp/server$i-sorted 2>/dev/null
	grep latency $dir/server$i*.log  > /tmp/server.$$
	awk ' { print $12 " " $8 " " $14 } ' /tmp/server.$$ | sort -g > /tmp/server$i-sorted
	echo "create /tmp/server$i-sorted"
done
rm /tmp/server.$$
