#!/usr/bin/env python
import os
import sys
import re
from datetime import datetime

from argparse import ArgumentParser
import pandas as pd

time_re = re.compile(r'(\d+)m(\d*\.?\d+)s')


def parse_datetime(datetime_str):
    return datetime.strptime(datetime_str.strip()[:-3], '%m/%d/%y %H:%M:%S.%f')


def parse_runtime(time_str):
    r = time_re.findall(time_str)[0]
    if len(r) == 2:
        minute, second = float(r[0]), float(r[1])
        return minute * 60 + second
    return None


def get_bench_time(path, comb_dir_name):
    result = list()
    copy_time = dict()
    comb_dir_path = os.path.join(path, comb_dir_name)
    comb = comb_dir_name.replace('/', ' ').strip()
    i = iter(comb.split('-'))
    comb = dict(zip(i, i))
    data = list()
    try:
        src_site = ""
        for file_name in os.listdir(comb_dir_path):
            cur_row = comb.copy()
            if '_bench' in file_name:
                cur_row['hostname'] = file_name.split('_bench')[0].strip()

                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    lines = [parse_runtime(line.strip().split('\t')[-1])
                             for line in f if line.strip() and 'real' in line]
                    if len(lines) == 1:
                        cur_row['time_bench'] = lines[0]
                data.append(cur_row)
    except IOError as e:
        print('[ERROR][BootTime-IO]', str(e))
    return data


def get_convergence_time(path, comb_dir_name):
    result = list()
    convergence_time = dict()
    comb_dir_path = os.path.join(path, comb_dir_name)
    comb = comb_dir_name.replace('/', ' ').strip()
    i = iter(comb.split('-'))
    comb = dict(zip(i, i))
    data = list()
    try:
        src_site = ""
        cur_row = comb.copy()
        for file_name in os.listdir(comb_dir_path):
            if '_start' in file_name:
                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    cur_row['time_cp_start'] = parse_datetime(f.readline())
                    cur_row['time_cp_end'] = parse_datetime(f.readline())
                    cur_row['src_host'] = file_name.split('_')[1]
                    src_site = cur_row['src_host'].split('-')[0]
            elif 'checksum_copy_' in file_name:
                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    cur_row['copy_ok'] = f.readline().strip()
            elif 'checkelmerfs_' in file_name:
                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    cur_row['elmerfs_ok'] = 0
                    if "rpfs on" in f.readline().strip():
                        if "rpfs on" in f.readline().strip():
                            cur_row['elmerfs_ok'] = 1
        for file_name in os.listdir(comb_dir_path):
            if '_end' in file_name:
                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    cur_end_row = cur_row.copy()
                    cur_end_row['end_time'] = parse_datetime(f.readline())
                    host_name = file_name.split('_')[1]
                    if src_site in host_name:
                        cur_end_row['host_inner'] = host_name
                    else:
                        cur_end_row['host_outer'] = host_name
                data.append(cur_end_row)
    except IOError as e:
        print('[ERROR][BootTime-IO]', str(e))
    return data


def get_copy_time(path, comb_dir_name):
    result = list()
    copy_time = dict()
    comb_dir_path = os.path.join(path, comb_dir_name)
    comb = comb_dir_name.replace('/', ' ').strip()
    i = iter(comb.split('-'))
    comb = dict(zip(i, i))
    data = list()
    try:
        src_site = ""
        cur_row = comb.copy()
        for file_name in os.listdir(comb_dir_path):
            if '_start' in file_name:
                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    cur_row['time_cp_start'] = parse_datetime(f.readline())
                    cur_row['time_cp_end'] = parse_datetime(f.readline())
                    cur_row['src_host'] = file_name.split('_')[1]
                    src_site = cur_row['src_host'].split('-')[0]
            elif 'checksum_copy_' in file_name:
                with open(os.path.join(comb_dir_path, file_name), "r") as f:
                    cur_row['copy_ok'] = f.readline().strip()
        data.append(cur_row)
    except IOError as e:
        print('[ERROR][BootTime-IO]', str(e))
    return data


def process_convergence_time(df):
    # calculate delta time
    df['copy_time'] = df['time_cp_end'] - df['time_cp_start']
    df['convergence_time'] = df['end_time'] - df['time_cp_start']
    df['copy_time'] = df['copy_time'].dt.total_seconds()
    df['convergence_time'] = df['convergence_time'].dt.total_seconds()
    df['latency'] = df['latency'].astype(int)
    df['iteration'] = df['iteration'].astype(int)
    sort_by = ['benchmarks', 'latency', 'iteration', 'src_host']
    columns = ['benchmarks', 'latency', 'iteration', 'src_host']
    if 'host_inner' in df:
        sort_by.append('host_inner')
        columns.append('host_inner')
    if 'host_outer' in df:
        sort_by.append('host_outer')
        columns.append('host_outer')
    if 'node' in df:
        sort_by.append('node')
        columns.append('node')
    sort_by.append('end_time')
    columns += ['time_cp_start', 'time_cp_end', 'end_time', 'copy_time', 'convergence_time']
    df.sort_values(by=sort_by, inplace=True)
    if "copy_ok" in df:
        columns.append("copy_ok")
    if "elmerfs_ok" in df:
        columns.append("elmerfs_ok")

    return df[columns]


def main(options):
    parser = ArgumentParser(prog='parse_result_elmerfs')

    parser.add_argument('-i', '--input', dest='input', type=str, required=True,
                        help='The path to the result directory.')
    parser.add_argument('-o', '--output', dest='output', type=str, required=True,
                        help='The path to the output result file.')
    parser.add_argument('--convergence', dest='parse_convergence', action='store_true',
                        help='Parse convergence time')
    parser.add_argument('--copy', dest='parse_copy', action='store_true',
                        help='Parse copy time')
    parser.add_argument('--bench', dest='parse_bench', action='store_true',
                        help='Parse bench time')

    args = parser.parse_args()

    print('Running with the following parameters: %s' % args)

    # read input
    print('Reading input directory', args.input)
    list_comb = os.listdir(args.input)
    result = list()
    speed = list()
    for comb in list_comb:
        if comb not in ['sweeps', 'stdout+stderr', 'graphs', '.DS_Store']:
            if os.path.isdir(os.path.join(args.input, comb)):
                if args.parse_convergence:
                    result += get_convergence_time(args.input, comb)
                elif args.parse_copy:
                    result += get_copy_time(args.input, comb)
                elif args.parse_bench:
                    result += get_bench_time(args.input, comb)

    print('Total %s result rows' % len(result))
    df = pd.DataFrame(result)
    if args.parse_convergence:
        df = process_convergence_time(df)
    elif args.parse_copy:
        df['copy_time'] = df['time_cp_end'] - df['time_cp_start']
        df['copy_time'] = df['copy_time'].dt.total_seconds()
    print('Exporting to', args.output)
    df.to_csv(args.output, index=False)


if __name__ == "__main__":
    main(sys.argv[1:])
