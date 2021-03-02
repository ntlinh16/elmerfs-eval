#!/bin/bash

set -u
set -x

MOUNTPOINT=$1
COMMON="${MOUNTPOINT}/common"
BENCH_USER="$(hostname)-efs"
FILE_SIZE=$((10*1024*1024))
READY="${MOUNTPOINT}/ready"

rm -rf "${COMMON}"

# Create a directory common for all benchmarks

mkdir "${COMMON}"
chmod 777 "${COMMON}"

seq 1 100 | xargs -P 4 -I {} touch "${COMMON}/f{}"
seq 1 100 | xargs -P 4 -I {} chmod 644 "${COMMON}/f{}"

# Create a file with some content that multiple nodes will be able to read
BLOCK_SIZE=65536
COUNT=$(($FILE_SIZE/$BLOCK_SIZE))

dd if=/dev/urandom of="${COMMON}/fx" bs="${BLOCK_SIZE}" count="${COUNT}"
chmod 644 "${COMMON}/fx"

# Create a ready folder, to ensure that each node will start at
# approximatively the same time.

mkdir "${READY}"
chmod 777 "${READY}"