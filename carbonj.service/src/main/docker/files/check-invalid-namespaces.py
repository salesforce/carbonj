#!/usr/bin/env python3
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


import os
import logging
import subprocess
import time
import glob
import argparse


logger = logging.getLogger()


def setup_logging():
    logger.setLevel(level=os.environ.get("LOGLEVEL", "INFO"))
    console = logging.StreamHandler()
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)


def get_key_value(line):
    if '==>' in line:
        key, value = line.split('==>')
        return key.strip(), value.strip()
    return line.strip(), None


def run_command(command, retry=1):
    count = 1
    while count <= retry:
        count += 1
        try:
            line = subprocess.check_output(command, text=True, stderr=subprocess.STDOUT)
            return line.strip()
        except subprocess.CalledProcessError as e:
            logger.error(f"Error: {e}")
            time.sleep(1)
    return ''


def check_id_in_db(db_name, key):
    command = ['/root/ldb', 'scan', f"--db=/data/carbonj-data/{db_name}", f"--from={key}", '--max_keys=1', '--hex', '--no_value']
    line = run_command(command, 3)
    logger.info('Running command: %s', command)
    if line == '' or len(line) < len(key):
        return False
    real_key = line[:len(key)]
    if real_key != key:
        logger.info('Mismatched real key %s', real_key)
        return False
    return True


def parse_args():
    parser = argparse.ArgumentParser(description='Process RocksDB invalid ID check.')
    parser.add_argument('--common-prefix', dest='common_prefix', help='Check IDs with the common prefix', default='')
    parser.add_argument('--skip-prefix', dest='skip_prefix', help='Comma separated prefixes to skip', default='')
    parser.add_argument('--no-dump', dest='no_dump', help='Whether to dump RocksDB or not', action='store_true')
    parser.add_argument('--check-count', dest='check_count', type=int, help='The number of invalid checks', default='10')
    return parser.parse_args()


setup_logging()


class RocksDbKeyDumper:
    def __init__(self, db_name, args):
        self.db_name = db_name
        self.file_number = 0
        self.no_dump = args.no_dump
        self.common_prefix = args.common_prefix
        if not self.common_prefix.endswith('.'):
            self.common_prefix += '.'
        self.prefix_parts = len(self.common_prefix.split('.'))
        self.skip_prefixes = args.skip_prefix.split(',')
        self.check_count = args.check_count

    def dump(self, include_value=True):
        if self.no_dump:
            logger.info('Skipping dump')
            return
        self.cleanup_files()
        last_key = ''
        while True:
            if last_key == '':
                command = ['/root/ldb', 'scan', f"--db=/data/carbonj-data/{self.db_name}", '--max_keys=100000000', '--hex']
            else:
                command = ['/root/ldb', 'scan', f"--db=/data/carbonj-data/{self.db_name}", f"--from={last_key}", '--max_keys=100000000', '--hex']
            if not include_value:
                command.append('--no_value')
            logger.info('Running command: %s', command)
            self.file_number += 1
            start_time = time.time()
            file = f"/data/{self.db_name}_scan.{self.file_number}"
            with open(file, 'w') as f:
                try:
                    subprocess.run(command, stdout=f)
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error: {e}")
                finally:
                    elapsed_time = time.time() - start_time
                    logger.info(f"Elapsed time to dump {self.db_name} keys to file {file} : {elapsed_time}")
            command = ['tail', '-1', file]
            line = run_command(command)
            key, value = get_key_value(line)
            if key == last_key:
                break
            last_key = key

    def check_invalid_ids(self):
        invalid_id_count = 0
        stop = False
        checked_prefixes = []
        prefix = ''
        files = sorted(glob.glob(f"/data/{self.db_name}_scan.*"))
        with open('/data/invalid_namespaces', 'a', buffering=1) as wf:
            for file in files:
                if stop:
                    break
                logger.info('Scanning IDs in file %s', file)
                with open(file, 'r') as f:
                    while True:
                        line = f.readline()
                        if not line:
                            break
                        key, value = get_key_value(line)
                        name = bytes.fromhex(value[2:]).decode('utf-8')
                        first = name.split('.')[0]
                        # Skip root check
                        if name == 'root':
                            continue
                        if self.common_prefix != '.':
                            if not name.startswith(self.common_prefix):
                                continue
                            prefix = '.'.join(name.split('.')[:self.prefix_parts])
                            if prefix in checked_prefixes:
                                continue
                        else:
                            if first in self.skip_prefixes:
                                continue

                        logger.info('Checking namespace %s', name)
                        valid_id = False
                        for db_name in ['30m2y', '5m7d', '60s24h']:
                            if check_id_in_db(db_name, key):
                                valid_id = True
                                break
                        if not valid_id:
                            wf.write(f"{name}\n")
                            invalid_id_count += 1
                            if invalid_id_count >= self.check_count:
                                invalid_id_count = 0
                                if self.common_prefix == '.':
                                    self.skip_prefixes.append(first)
                                else:
                                    checked_prefixes.append(prefix)
                        else:
                            if self.common_prefix == '.':
                                self.skip_prefixes.append(first)
                            else:
                                checked_prefixes.append(prefix)

    def cleanup_files(self):
        files = glob.glob(f"/data/{self.db_name}_scan.*")
        files.append('/data/invalid_namespaces')
        for file in files:
            try:
                os.remove(file)
            except Exception as e:
                logger.error(f"Error: {e}")


class Main:
    def __init__(self):
        return

    def execute(self):
        args = parse_args()
        dumper = RocksDbKeyDumper('index-id', args)
        dumper.dump()
        dumper.check_invalid_ids()


# Documentation https://salesforce.quip.com/gO3FAhYxHpQX
if __name__ == "__main__":
    Main().execute()
