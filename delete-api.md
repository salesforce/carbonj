# Run-Book Delete metrics

## Background

We have delete admin api exposed on carbonj and have script to call delete on all the shards and replicas. Why and how we would like to delete metrics from metrics cloud has been discussed here :  [Metrics Cloud Admin API - DELETE](https://salesforce.quip.com/6xX7ApHWYbWM)

This document is to show how we can delete metrics from metrics cloud.

## Script to run & location

You need to run the script *deletemetrics.py* located at /dw/ in one of the carbonj container

* Enter into one of the the carbonj container - kubectl exec -it carbonj-p1-0 -n carbonj sh
* cd /dw/
* Run the script ./deletemetrics.py

## Delete metric with prefix

When you run the script, it asks for metric name to be deleted. You can either give a full metric name(leaf) or you can give a non leaf node metric name.

### metric name with leaf node

Deletes a single metric. Ex:  `What's the metric you would like to delete : pod30.ecom.session.count`
deletes only that metric.

### Metric name with non leaf node

Deletes all the children of that node. Ex: `What's the metric you would like to delete : pod30`
deletes all the metrics under pod30. 

### Prefix with wild card

`What's the metric you would like to delete : pod30.*.*.count`
`What's the metric you would like to delete : pod*.*.*.count`
removes metric ending count with depth 3 across all the pods. Should be careful when we use it this.

## Delete metric with suffix

Suffix delete is triggered if the first node of the given metric is `**`
`What's the metric you would like to delete : **.order.data`
Finds all the segments from the root that contains first occurrence of the order.data and deletes all of them. We should exercise this with care as this may parse the full tree in the worst case and is very expensive. Avoid using this if possible.

## Exclude nodes from deletion

Script allows to exclude nodes from deletion. You can give list of node that you don't want to delete

```
What's the metric you would like to delete : pod20
What are the metrics you would like to exclude from deletion Ex : a,b : ecom_ag
```

In this case all the metics under pod20 get deleted except ecom_ag.

## Dry run or Delete

Script allows to do the dry run before actual delete. Response will have total nodes count and leaf node count. If it is run with delete mode, it actually deletes the metrics

```
 Would you like to execute delete operation on all the shards ? yes or No : yes
```

## Target specific shard & replica

Script allows to target certain shard & replica for deletion. It also has an option to execute delete on all the shards and replicas.

For all the shards and replicas

```
Would you like to execute delete operation on all the shards ? yes or No : yes
```

For specific shards and replicas

```
What are the shrards you would like to delete metrics on Ex: 1, 2, 3? 1,2
What are the replicas you would like to delete metrics on Ex: 0,1? 0,1
```

## Sample execution

Delete all the metrics from pod28 except ecom_ag.

```
sh-4.2# cd dw/
sh-4.2# ./deletemetrics.py
What's the metric you would like to delete : pod28
What are the metrics you would like to exclude from deletion Ex : a,b : ecom_ag
Do you want to delete the metric pod28 or Do the dry run : yes[delete]  or No[dry-run] : yes
Would you like to execute delete operation on all the shards ? yes or No : yes

Execution is being called on these shard-replicas :
['http://carbonj-p1-0.carbonj-p1.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p1-1.carbonj-p1.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p2-0.carbonj-p2.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p2-1.carbonj-p2.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p3-0.carbonj-p3.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p3-1.carbonj-p3.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p4-0.carbonj-p4.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p4-1.carbonj-p4.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p5-0.carbonj-p5.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true',
 'http://carbonj-p5-1.carbonj-p5.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true']

Do you really want to proceed with delete yes or NO ? : yes
{'url': 'http://carbonj-p4-0.carbonj-p4.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 0, 'leafCount': 0, 'success': True, 'error': None, 'metricsList': [], 'metrics': [], 'deleteBranch': True, 'responseTtime': 0}}
{'url': 'http://carbonj-p3-1.carbonj-p3.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 0, 'leafCount': 0, 'success': True, 'error': None, 'metricsList': [], 'metrics': [], 'deleteBranch': True, 'responseTtime': 0}}
{'url': 'http://carbonj-p5-1.carbonj-p5.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 0, 'leafCount': 0, 'success': True, 'error': None, 'metricsList': [], 'metrics': [], 'deleteBranch': True, 'responseTtime': 0}}
{'url': 'http://carbonj-p5-0.carbonj-p5.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 0, 'leafCount': 0, 'success': True, 'error': None, 'metricsList': [], 'metrics': [], 'deleteBranch': True, 'responseTtime': 0}}
{'url': 'http://carbonj-p3-0.carbonj-p3.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 0, 'leafCount': 0, 'success': True, 'error': None, 'metricsList': [], 'metrics': [], 'deleteBranch': True, 'responseTtime': 0}}
{'url': 'http://carbonj-p4-1.carbonj-p4.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 0, 'leafCount': 0, 'success': True, 'error': None, 'metricsList': [], 'metrics': [], 'deleteBranch': True, 'responseTtime': 0}}
{'url': 'http://carbonj-p1-1.carbonj-p1.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 910048, 'leafCount': 531426, 'success': True, 'error': None, 'metricsList': ['pod28'], 'metrics': [], 'deleteBranch': True, 'responseTtime': 292376}}
{'url': 'http://carbonj-p1-0.carbonj-p1.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 910048, 'leafCount': 531426, 'success': True, 'error': None, 'metricsList': ['pod28'], 'metrics': [], 'deleteBranch': True, 'responseTtime': 296652}}
{'url': 'http://carbonj-p2-1.carbonj-p2.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 2145235, 'leafCount': 1316952, 'success': True, 'error': None, 'metricsList': ['pod28'], 'metrics': [], 'deleteBranch': True, 'responseTtime': 681003}}
{'url': 'http://carbonj-p2-0.carbonj-p2.carbonj.svc.cluster.local:2001/_dw/rest/carbonj/metrics/deleteAPI/pod28?exclude=ecom_ag&delete=true', 'status': 200, 'response': {'totalCount': 2145233, 'leafCount': 1316951, 'success': True, 'error': None, 'metricsList': ['pod28'], 'metrics': [], 'deleteBranch': True, 'responseTtime': 704619}}

 !!!!! Execution complete. You can find the results here: /dw/delete-2019-08-2113:32:53.045084.txt !!!!!
```

