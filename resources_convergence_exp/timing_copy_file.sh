#!/bin/bash

file_path="$1"
dest_path="$2"
log_path="$3"
hash="$4"
checksum_path="$5"
mount_check_path="$6"

# check if elmerfs is running
now=$(date +"%D %T.%N")
checkmount=$(mount | grep dc-)
echo "start:$now -> $checkmount" >> $mount_check_path

# save the t_start before copying
echo $(date +"%D %T.%N") >> $log_path

# save the t_start2 after finishing copying
cp $file_path $dest_path && echo $(date +"%D %T.%N") >> $log_path

# check if elmerfs is running
now=$(date +"%D %T.%N")
checkmount=$(mount | grep dc-)
echo "end:$now -> $checkmount" >> $mount_check_path

cur_hash=$(sha256sum $dest_path | awk '{print $1}')
echo $cur_hash

# verify the hash
if [ $hash == $cur_hash ]; then
    echo "checksum OK"
    echo "1" >> $checksum_path
else
    echo "checksum failed"
    echo "0" >> $checksum_path
fi