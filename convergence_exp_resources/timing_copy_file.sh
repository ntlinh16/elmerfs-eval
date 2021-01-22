#!/bin/bash

file_path="$1"
dest_path="$2"
log_path="$3"

# save the t_start before copying
echo $(date +"%D %T.%N") >> $log_path

# save the t_start2 after finishing copying
cp $file_path $dest_path && echo $(date +"%D %T.%N") >> $log_path

