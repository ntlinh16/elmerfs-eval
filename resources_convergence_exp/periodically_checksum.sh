#!/bin/bash

file_path="$1"
hash="$2"
log_path="$3"

i=0
while true
do
    echo i: $i
    let "i=i+1"
    echo $file_path
    echo $hash
    # compute the current hash of the file
    cur_hash=$(sha256sum $file_path | awk '{print $1}')
    echo $cur_hash

    # verify the hash
    if [ $hash == $cur_hash ]; then
        echo "checksum OK"
	# save the t_end time
	echo $(date +"%D %T.%N") > $log_path
	break
    fi
    	
    sleep 2s
done
