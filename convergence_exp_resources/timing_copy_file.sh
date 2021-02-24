#!/bin/bash

file_path="$1"
dest_path="$2"
log_path="$3"
hash="$4"
checksum_path="$5"

# save the t_start before copying
echo $(date +"%D %T.%N") >> $log_path

# save the t_start2 after finishing copying
cp $file_path $dest_path && echo $(date +"%D %T.%N") >> $log_path

cur_hash=$(sha256sum $dest_path | awk '{print $1}')
echo $cur_hash

# verify the hash
if [ $hash == $cur_hash ]; then
    echo "checksum OK"
    echo "1" >> $checksum_path
fi
