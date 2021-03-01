#!/bin/bash

# passing in x.y and output x*100000000 + y.

convert()
{
	t1=`echo $1 | tr '.' ' ' | awk '{ print $1 }'`	
	t2=`echo $1 | tr '.' ' ' | awk '{ print $2 }'`	
	result=`expr $t2 + $t1 \* 1000000000`
	# echo $result 
}

if [ $# -eq 1 ]; then
	dir=$1
	num=0;
else 
	if [ $# -eq 2 ]; then
		dir=$1
		num=$2
	else
		echo "Usage: latency.sh <dir> <cnt>"
	fi
fi

cnt=$(ls $dir/server*.*.log | wc -l)
for i in $(seq 1 $cnt); do
	rm /tmp/server$i-sorted 2>/dev/null
	grep -a latency $dir/server$i.*.log  > /tmp/server.$$
	awk ' { print $12 " " $8 " " $14 } ' /tmp/server.$$ | sort -g > /tmp/server$i-sorted
	echo "create /tmp/server$i-sorted"
done

rm /tmp/server.$$

if [[ $num -eq 0 ]]; then
    exit 0;
fi

#for line in `tail -n $num /tmp/server1-sorted`; do
filename=/tmp/trail-list.$$
tail -n $num /tmp/server1-sorted > $filename
printf "Latency\tCTS\t\t\tTX-Type\tAct-Lat\tRecv1-Lat\tRecv2-Lat\n"
while IFS= read -r line; do 
	lat=`echo $line | awk '{ print $1 }'`
	cts=`echo $line | awk '{ print $2 }'`
	txx=`echo $line | awk '{ print $3 }'`

	t=`grep -a $cts $dir/server1.*.log | grep insertTxEntry | awk '{ print $1 }'`;
	if [[ ! -z "$t" ]]; then 
		convert $t
		insert_t=`echo $result`;
	else 
		# missing insert, skip
		continue;
	fi
	t=`grep -a $cts $dir/server1.*.log | grep -m 1 activate | awk '{ print $1 }'`;
	if [[ ! -z "$t" ]]; then 
		convert $t
		act_t=`echo $result`;
		act_l=`expr $act_t - $insert_t`
		act_l=`expr $act_l / 1000`
	else
		act_l="NULL";
	fi
	t=`grep -a $cts $dir/server1.*.log | grep receiveSSNInfo | grep "from 2" -m 1`
	if [[ ! -z "$t" ]]; then 
		convert $t
		recv1_t=`echo $result`;
		recv1_l=`expr $recv1_t - $insert_t`
		recv1_l=`expr $recv1_l / 1000`
	else
		recv1_l="NULL"
	fi
	t=`grep -a $cts $dir/server1.*.log | grep receiveSSNInfo | grep "from 3" -m 1`
	if [[ ! -z "$t" ]]; then 
		convert $t
		recv2_t=`echo $result`;
		recv2_l=`expr $recv2_t - $insert_t`
		recv2_l=`expr $recv2_l / 1000`
	else
		recv2_l="NULL";
	fi
	printf "%s\t%s\t%s\t\t%s\t%s\t%s\n" $lat $cts $txx $act_l $recv1_l $recv2_l  
done < "$filename" 
rm $filename
