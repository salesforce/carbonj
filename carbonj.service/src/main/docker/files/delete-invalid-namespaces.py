#!/usr/bin/env python3

import os
import logging
import argparse
import requests


logger = logging.getLogger()


def setup_logging():
    logger.setLevel(level=os.environ.get("LOGLEVEL", "INFO"))
    console = logging.StreamHandler()
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(filename)s:%(lineno)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)


def parse_args():
    parser = argparse.ArgumentParser(description='Delete RocksDB invalid namespaces.')
    parser.add_argument('--shard', dest='shard', help='The shard ID', required=True)
    parser.add_argument('--replica', dest='replica', help='The replica ID', required=True)
    parser.add_argument('--invalid-namespaces-file', dest='invalid_namespaces_file', help='Invalid namespaces file', required=True)
    parser.add_argument('--prefix-parts', dest='prefix_parts', type=int, help='The number of dot separated parts as prefix', default='0')
    parser.add_argument('--dry-run', dest='dry_run', help='Whether to dryrun', action='store_true')
    return parser.parse_args()


setup_logging()


class InvalidNamespacesCleaner:
    def __init__(self, args):
        self.shard = args.shard
        self.replica = args.replica
        self.invalid_namespaces_file = args.invalid_namespaces_file
        self.prefix_parts = args.prefix_parts
        self.dry_run = args.dry_run

    def clean(self):
        deleted_namespaces = []
        with open(self.invalid_namespaces_file, 'r') as f:
            while True:
                namespace = f.readline()
                if not namespace:
                    break
                namespace = namespace.strip()
                if not namespace:
                    continue
                if self.prefix_parts > 0:
                    namespace = '.'.join(namespace.split('.')[:self.prefix_parts])
                if namespace in deleted_namespaces:
                    continue
                logger.info(f"Start cleaning namespace {namespace} with shard: {self.shard} on replica: {self.replica}")
                deleted_namespaces.append(namespace)
                if not self.dry_run:
                    self.delete_namespace(namespace)
                logger.info(f"Done with cleaning namespace {namespace} with shard: {self.shard} on replica: {self.replica}")

    def delete_namespace(self, namespace):
        url = f"http://carbonj-p{self.shard}-{self.replica}.carbonj-p{self.shard}.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/{namespace}?exclude=&delete=true"
        response = requests.delete(url)
        if response.status_code == 200:
            logger.info(response.json())
        else:
            logger.error(f"Failed to delete namespace {namespace} with HTTP Status code {response.status_code}")


class Main:
    def __init__(self):
        return

    def execute(self):
        args = parse_args()
        cleaner = InvalidNamespacesCleaner(args)
        cleaner.clean()


# Documentation https://salesforce.quip.com/gO3FAhYxHpQX
if __name__ == "__main__":
    Main().execute()
