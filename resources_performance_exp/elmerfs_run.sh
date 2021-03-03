#!/bin/bash

set -u

MOUNTPOINT=$1
RESULTS=$2
EXPECTED_NODE_COUNT=$3
ELMERFS_UID=$4

STOPF="/tmp/stop"
COMMON="${MOUNTPOINT}/common"
BENCH_USER="$(hostname)_bench"
READY="${MOUNTPOINT}/ready"
N_TIMES=5

function wait_ready {
    local EXPECTED_NODE_COUNT=$1
    local N=50 

    while [ ! -f "${STOPF}" ]; do
        if [ "$N" -eq 0 ]; then
            touch "${RESULTS}/${BENCH_USER}_notReady.txt"
            echo "Cannot wait until ready."
            exit
        fi

        local FOUND=$(ls -1 ${READY} | wc -l)
        if [ "${FOUND}" == "${EXPECTED_NODE_COUNT}" ]; then
            break
        fi
        ((N=N-1))

        echo "Waiting for readiness... sleep"
        sleep 3

    done

    echo "Ready."
    touch "/tmp/$(hostname)_ready.txt"
}

function repeat {
    local REMAINING="${N_TIMES}"
    while [ ! -f "${STOPF}" ] && [ $REMAINING -gt 0 ]; do
        $@
        REMAINING=$(($REMAINING-1))
    done
}

function once {
    if [ ! -f "${STOPF}" ]; then
        $@
    fi
}

function bench {
    # Some listing on a shared folder
    repeat ls -ali "${COMMON}" > /dev/null

    # Write and read a file
    local WRITE_FILE="${COMMON}/${BENCH_USER}.write"
    local BLOCK_COUNT=1024
    once dd if=/dev/urandom of="${WRITE_FILE}" count="${BLOCK_COUNT}" 2> /dev/null
    repeat cat "${WRITE_FILE}" > /dev/null

    # Create a file that will have a name conflict
    local IN_CONFLICT_FILE="${COMMON}/in_conflict"
    once touch "${IN_CONFLICT_FILE}"
    once rm "${IN_CONFLICT_FILE}"

    # Create inodes in the common directory to stress concurrent
    # ops.
    if [ ! -f "${STOPF}" ]; then
        seq 0 100 | xargs -n 1 -P 8 -I {} mkdir "${COMMON}/d${BENCH_USER}{}"
    fi
}

# 1. Operations will be done as an user specific to each node
#useradd "${BENCH_USER}" -u "${ELMERFS_UID}" || true

# 2. Wait for every node that will run the actual benchmark to be up and
# waiting
rm "/tmp/$(hostname)_ready.txt"
touch "${READY}/${HOSTNAME}"
wait_ready "${EXPECTED_NODE_COUNT}"

# 3. Run the actual benchmark and record how much time it takes
{ time bench ; } 2>&1 | tee "${RESULTS}/${BENCH_USER}.txt"
