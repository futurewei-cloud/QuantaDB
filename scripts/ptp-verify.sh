#!/bin/sh

# Copyright 2020 Futurewei Technologies, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

chmod +x scripts/config.py

for i in `scripts/config.py`; do
	f1=0
	f2=0
	rsh $i service phc2sys status 2>/dev/null 1>&2;
	if [ $? -ne 0 ]; then
		f1=1
		echo "---- no phc2sys on $i ----" 
	fi
	rsh $i service ptp4l status 2>/dev/null 1>&2;
	if [ $? -ne 0 ]; then
		f2=1
		echo "---- no p4p4l on $i ----" 
	fi
	rsh $i ls -l /dev/ptp1 | grep "crw-r--r--" 2>/dev/null 1>&2 
	if [ $? -ne 0 ]; then
		echo "---- /dev/ptp1 permission not right on $i ---- "
	else 
		if [ $f1 -eq 0 ];  then
			if [ $f2 -eq 0 ]; then
				echo "---- ptp is good on $i ---"  
			fi
		fi
	fi
done
chmod -x scripts/config.py
