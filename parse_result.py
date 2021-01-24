#!/usr/bin/env python
import os
import sys
from argparse import ArgumentParser
import pandas as pd
from datetime import datetime


def parse_datetime(datetime_str):
    return datetime.strptime(datetime_str.strip()[:-3], '%m/%d/%y %H:%M:%S.%f')


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


def process_convergence_time(df):
    # calculate delta time
    df['copy_time'] = df['time_cp_end'] - df['time_cp_start']
    df['convergence_time'] = df['end_time'] - df['time_cp_start']
    df['copy_time'] = df['copy_time'].dt.total_seconds()
    df['convergence_time'] = df['convergence_time'].dt.total_seconds()
    df['latency'] = df['latency'].astype(int)
    df['iteration'] = df['iteration'].astype(int)
    df.sort_values(by=['benchmarks', 'latency', 'iteration',
                       'src_host', 'host_inner', 'host_outer', 'end_time'], inplace=True)
    return df[['benchmarks', 'latency', 'iteration', 'src_host', 'host_inner', 'host_outer',
               'time_cp_start', 'time_cp_end', 'end_time', 'copy_time', 'convergence_time']]


def main(options):
    parser = ArgumentParser(prog='parse_result_elmerfs')

    parser.add_argument('-i', '--input', dest='input', type=str, required=True,
                        help='The path to the result directory.')
    parser.add_argument('-o', '--output', dest='output', type=str, required=True,
                        help='The path to the output result file.')
    parser.add_argument('--convergence', dest='parse_convergence', action='store_true',
                        help='Parse convergence time')

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

    print('Total %s result rows' % len(result))
    df = pd.DataFrame(result)
    if args.parse_convergence:
        df = process_convergence_time(df)
    print('Exporting to', args.output)
    df.to_csv(args.output, index=False)


if __name__ == "__main__":
    main(sys.argv[1:])
