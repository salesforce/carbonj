#! /usr/bin/env python3.6
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#


import asyncio
import aiohttp
import datetime
from urllib.parse import urlencode
from pathlib import Path

prd_base_delete_url = 'http://carbonj-{shard}-{replica}.carbonj-{shard}.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/{metric_id}?'
local_base_delete_url = 'http://localhost:56687/_dw/rest/carbonj/metrics/deleteAPI/{}?'
shard_count = 15 
is_prd = True
file_name = ''.join(["delete-", str(datetime.datetime.now().date()), str(datetime.datetime.now().time()), ".txt"])


def _format_url_r(base_delete_url, metric_id, arguments):
    return ''.join([base_delete_url.format(metric_id), urlencode(arguments)])


def _format_url(base_delete_url, shard, replica, metric_id, arguments):
    return ''.join([base_delete_url.format(shard=shard, replica=replica, metric_id=metric_id), urlencode(arguments)])


def _get_urls_all_shards(base_delete_url, metric_id, arguments):
    url_list = []
    for p in range(shard_count):
        for r in range(2):
            url_list.append(_format_url(base_delete_url, 'p' + str(p+1), str(r), metric_id, arguments))
    return url_list


def _get_urls(shards, replicas, base_delete_url, metric_id, arguments):
    if not shards:
        return _get_urls_all_shards(base_delete_url, metric_id, arguments)

    url_list = []
    for p in shards:
        for r in replicas:
            url_list.append(_format_url(base_delete_url, 'p' + str(p), str(r), metric_id, arguments))
    return url_list


def _get_int_list(list_name, message):
    try:
        input_list = list(map(int, input(message).split(',')))
        return input_list
    except:
        print("{} should be an integer list.".format(list_name))
        return _get_int_list(list, message)


def _get_shards_replicas():
    shards = _get_int_list("shards", "What are the shrards you would like to delete metrics on Ex: 1, 2, 3? ")
    replicas = []
    if shards:
        replicas = _get_int_list("replicas", "What are the replicas you would like to delete metrics on Ex: 0,1? ")
    return shards, replicas


def _get_args():
    metric_id = input("What's the metric you would like to delete : ")
    exclusions = input("What are the metrics you would like to exclude from deletion Ex : a,b : ")
    do_delete = input("Do you want to delete the metric {} or Do the dry run : yes[delete]  or No[dry-run] : ".format(metric_id))

    if not is_prd:
        return [], [], metric_id, exclusions, do_delete

    shards_provided = input("Would you like to execute delete operation on all the shards ? yes or No : ")
    if 'yes' == shards_provided.lower() or 'y' == shards_provided.lower():
        return [], [], metric_id, exclusions, do_delete

    shards, replicas = _get_shards_replicas()
    return shards, replicas, metric_id, exclusions, do_delete


def _build_url():
    shards, replicas, metric_id, exclude, do_delete = _get_args()
    arguments = [('exclude', e) for e in exclude.split(',')]
    if 'yes' == do_delete.lower() or 'y' == do_delete.lower():
        arguments.append(('delete', 'true'))
    else:
        arguments.append(('delete', 'false'))

    if is_prd:
        return _get_urls(shards, replicas, prd_base_delete_url, metric_id, arguments)
    else:
        url = _format_url_r(local_base_delete_url, metric_id, arguments)
        return [url]


async def _delete_metric(session, url, f):
    try:
        async with session.delete(url, timeout=None) as response:
            resp = await response.json()
            res = {'url' : url}
            res['status'] = response.status
            res['response'] = resp
            f.write(str(res) + "\n")
            print(res)
    except Exception as err:
        error_message = 'Delete Error : {} \n url : {}\n'.format(str(err), url)
        f.write(error_message)
        print(error_message)


async def _delete_all_metrics(url_list, f):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in url_list:
            task = asyncio.ensure_future(_delete_metric(session, url, f))
            tasks.append(task)
        await asyncio.gather(*tasks, return_exceptions=True)


def _do_delete(url_list):
    print('\nExecution is being called on these shard-replicas : \n{}\n'.format(url_list))
    do_delete = input('Do you really want to proceed with delete yes or NO ? : ')
    with open(file_name, 'w') as f:
        if 'yes' == do_delete.lower() or 'y' == do_delete.lower():
            asyncio.get_event_loop().run_until_complete(_delete_all_metrics(url_list, f))
            print("\n !!!!! Execution complete. You can find the results here: {} !!!!!".format(Path(file_name).absolute()))
        else:
            print('!!!! Execution aborted !!!!')


def main():
    _do_delete(_build_url())


if __name__ == '__main__':
    main()
