#!/bin/sh

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
