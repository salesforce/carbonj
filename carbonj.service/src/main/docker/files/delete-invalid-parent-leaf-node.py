#!/usr/bin/env python3
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


import os
import platform
import logging
import subprocess
import time
import requests
import argparse
import socket


logger = logging.getLogger()


def setup_logging():
    logger.setLevel(level=os.environ.get("LOGLEVEL", "INFO"))
    console = logging.StreamHandler()
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)


def parse_args():
    parser = argparse.ArgumentParser(description='Delete invalid parent leaf nodes')
    parser.add_argument('--dry-run', dest='dry_run', help='Whether to dryrun', action='store_true')
    return parser.parse_args()


setup_logging()


INVALID_PARENT_LEAF_NODE_ERROR_FILE = '/data/invalid_parent_leaf_nodes_error'
PROCESSED_INVALID_PARENT_LEAF_NODE_FILE = '/data/processed_invalid_parent_leaf_nodes'
NOT_PROCESSED_INVALID_PARENT_LEAF_NODE_FILE = '/data/not_processed_invalid_parent_leaf_nodes'


def run_command(command):
    try:
        subprocess.run(command, shell=True, check=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e}")


def run_command_with_output(command, retry=1):
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
    logger.info('Running command: %s', command)
    line = run_command_with_output(command, 3)
    if line == '':
        return True
    if len(line) < len(key):
        return False
    real_key = line[:len(key)]
    if real_key != key:
        logger.info('Mismatched real key %s', real_key)
        return False
    return True


class InvalidParentLeafNodeCleaner:
    def __init__(self, args):
        self.dry_run = args.dry_run

    def check_aws_client(self):
        if os.path.exists("/usr/local/bin/aws"):
            return
        logger.info("Download AWS client ...")
        command = [f'curl https://awscli.amazonaws.com/awscli-exe-linux-{platform.machine()}.zip -o ./awscliv2.zip']
        run_command(command)
        logger.info("Install AWS client ...")
        command = ['unzip -q ./awscliv2.zip && ./aws/install']
        run_command(command)
        command = ['rm -rf ./aws ./awscliv2.zip']
        run_command(command)

    def check_ldb_tool(self):
        if os.path.exists("/root/ldb"):
            return
        self.check_aws_client()
        logger.info("Download ldb tool ...")
        command = ['aws s3 cp s3://carbonj-tools-bucket/ldb /root/ldb && chmod +x /root/ldb']
        run_command(command)

    def process_invalid_parent_leaf_nodes(self, id, name, shard, replica):
        logger.info(f"Processing invalid parent leaf node {id} with name {name}")
        hex_id = f'0x{int(id):016X}'
        for db_name in ['30m2y', '5m7d', '60s24h']:
            if check_id_in_db(db_name, hex_id):
                return False
        if self.dry_run:
            url = f"http://carbonj-{shard}-{replica}.carbonj-{shard}.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/{name}?exclude=&delete=false"
        else:
            url = f"http://carbonj-{shard}-{replica}.carbonj-{shard}.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/{name}?exclude=&delete=true"
        logger.info(f"Deleting invalid parent leaf node {name} with url {url}")
        response = requests.delete(url)
        if response.status_code == 200:
            logger.info(response.json())
            return True
        else:
            logger.error(f"Failed to delete namespace {name} with HTTP Status code {response.status_code}")
            return False


    def dump_invalid_parent_leaf_nodes(self):
        self.check_ldb_tool()
        command = [f'grep ERROR /app/log/carbonj.log* | grep "Cannot create metric with name" > {INVALID_PARENT_LEAF_NODE_ERROR_FILE}']
        run_command(command)
        processed_invalid_parent_leaf_nodes = []
        if os.path.exists(PROCESSED_INVALID_PARENT_LEAF_NODE_FILE):
            with open(PROCESSED_INVALID_PARENT_LEAF_NODE_FILE, 'r') as f:
                while True:
                    line = f.readline()
                    if not line:
                        break
                    space = line.find(' ')
                    processed_invalid_parent_leaf_nodes.append(line[:space])

        processing_invalid_parent_leaf_nodes = {}
        with open(INVALID_PARENT_LEAF_NODE_ERROR_FILE, 'r') as f:
            while True:
                line = f.readline()
                if not line:
                    break
# /app/log/carbonj.log.2024-10-13.log:2024-10-13 23:59:49,456 ERROR c.d.c.s.d.i.MetricIndexImpl [TimeSeriesStore.SerialTaskPool 0]
# Cannot create metric with name [pod186.ecom.bgfq.bgfq_prd.blade3-6.bgfq_prd.healthmgr.threadCount.storefront]
# because [pod186.ecom.bgfq.bgfq_prd.blade3-6.bgfq_prd.healthmgr.threadCount] is already a leaf with ID [7353210610]
                left_bracket = line.rindex('[')
                right_bracket = line.rindex(']')
                id = line[left_bracket+1:right_bracket]
                if id in processed_invalid_parent_leaf_nodes or id in processing_invalid_parent_leaf_nodes:
                    continue
                left_bracket = line.rindex('[', 0, left_bracket)
                right_bracket = line.rindex(']', 0, right_bracket)
                name = line[left_bracket+1:right_bracket]
                processing_invalid_parent_leaf_nodes[id] = name

        hostname = socket.gethostname()
        parts = hostname.split('-')

        with open(PROCESSED_INVALID_PARENT_LEAF_NODE_FILE, 'a') as f, open(NOT_PROCESSED_INVALID_PARENT_LEAF_NODE_FILE, 'a') as nf:
            for id in processing_invalid_parent_leaf_nodes:
                if self.process_invalid_parent_leaf_nodes(id, processing_invalid_parent_leaf_nodes[id], parts[1], parts[2]):
                    f.write(f'{id} {processing_invalid_parent_leaf_nodes[id]}\n')
                else:
                    nf.write(f'{id} {processing_invalid_parent_leaf_nodes[id]}\n')


class Main:
    def __init__(self):
        return

    def execute(self):
        args = parse_args()
        cleaner = InvalidParentLeafNodeCleaner(args)
        cleaner.dump_invalid_parent_leaf_nodes()


if __name__ == "__main__":
    Main().execute()
