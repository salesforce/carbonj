#CarbonJ Admin API

###List Metrics Names

Format:

`
http://localhost:56787/_dw/rest/carbonj/listmetrics/{pattern}
`

Pattern format is the same as graphite patterns.

Example 1:

`
curl "http://localhost:56787/_dw/rest/carbonj/listmetrics/*"
`

Response:

`
[ "pod1", "pod2", "pod0", "pod3", "pod4", "pod98", "pod2", "pod4", "pod3", "pod1", "pod0" ]
`

Example 2:

`
curl "http://localhost:56787/_dw/rest/carbonj/listmetrics/*.\{ecom_ag,ocapi\}.*"
`

Note: `{`, `}` escaped for curl.

Response: 

`
[ "pod1x.ecom_ag.100", "pod2x.ecom_ag.200", "pod0x.ecom_ag.0", "pod3x.ecom_ag.300", "pod4x.ecom_ag.400", "pod2.ecom_ag.200", "pod4.ecom_ag.400", "pod3.ecom_ag.300", "pod1.ecom_ag.100", "pod0.ecom_ag.0" ]
`

###Get Metric data


Format:

`
http://localhost:56787/_dw/rest/carbonj/listpoints/{dbname}/{metricName}
`

At this time valid dbNames are "60s24h", "5m7d", "30m2y"

Example:

`
curl "http://localhost:56787/_dw/rest/carbonj/listpoints/60s24h/pod1x.ecom_ag.100.100_prd.rpc.outgoing.protocols.baskets.get.time.86sum"
`

`
[ "1463579460 -4124304", "1463580060 -954653" ]
`

###Get Metric data using internal metric id (to support testing)


Format:

`
http://localhost:2001/_dw/rest/carbonj/listpointswithid/{dbname}/{metricId}
`

At this time valid dbNames are "60s24h", "5m7d", "30m2y"

Example:

`
curl "http://localhost:56787/_dw/rest/carbonj/listpoints/60s24h/123"
`

`
[ "1463579460 -4124304", "1463580060 -954653" ]
`


### Get Metric definition

Format

`
http://localhost:56787/_dw/rest/carbonj/metrics/{metricName}
`

Examples:

Non-leaf metric name


`
curl "http://localhost:56787/_dw/rest/carbonj/metrics/pod1x.ecom_ag"
`

`
{
  "id" : 0,
  "name" : "pod1x.ecom_ag",
  "aggregationPolicy" : null,
  "retentionPolicies" : null,
  "highestPrecisionArchive" : {
    "present" : false
  },
  "leaf" : false,
  "maxRetention" : 0
}
`

Leaf metric name

`
curl "http://localhost:56787/_dw/rest/carbonj/metrics/pod1x.ecom_ag.100.100_prd.rpc.outgoing.protocols.baskets.get.time.86sum"
`

`
{
  "id" : 2258,
  "name" : "pod1x.ecom_ag.100.100_prd.rpc.outgoing.protocols.baskets.get.time.86sum",
  "aggregationPolicy" : {
    "method" : "AVERAGE",
    "xfactor" : 0.0
  },
  "retentionPolicies" : [ {
    "name" : "60s:24h",
    "precision" : 60,
    "retention" : 86400,
    "dbName" : "60s24h"
  }, {
    "name" : "5m:7d",
    "precision" : 300,
    "retention" : 604800,
    "dbName" : "5m7d"
  }, {
    "name" : "30m:2y",
    "precision" : 1800,
    "retention" : 63072000,
    "dbName" : "30m2y"
  } ],
  "highestPrecisionArchive" : {
    "present" : true
  },
  "leaf" : true,
  "maxRetention" : 63072000
}
`

###Search metric by id

Format

`
http://localhost:56787/_dw/rest/carbonj/metricsearch?metricId={id}
`

Example

`
curl "http://localhost:56787/_dw/rest/carbonj/metricsearch?metricId=2"
`

`
[ {
  "id" : 2,
  "name" : "pod1x.ecom.100.100_prd.blade-0.100_prd.rpc.outgoing.protocols.baskets.get.time.0mean",
  "aggregationPolicy" : {
    "method" : "AVERAGE",
    "xfactor" : 0.0
  },
  "retentionPolicies" : [ {
    "name" : "60s:24h",
    "precision" : 60,
    "retention" : 86400,
    "dbName" : "60s24h"
  }, {
    "name" : "5m:7d",
    "precision" : 300,
    "retention" : 604800,
    "dbName" : "5m7d"
  }, {
    "name" : "30m:2y",
    "precision" : 1800,
    "retention" : 63072000,
    "dbName" : "30m2y"
  } ],
  "highestPrecisionArchive" : {
    "present" : true
  },
  "leaf" : true,
  "maxRetention" : 63072000
} ]
`
###Find All Metric Names Recursively

Format

http://localhost:56787/_dw/rest/carbonj/findallmetrics/{metric name}

Example

`
curl "http://localhost:56787/_dw/rest/carbonj/findallmetrics/pod3x.ecom_ag"
`

###Delete metrics and associated data points

Format

`
DELETE http://localhost:56787/_dw/rest/carbonj/metrics/{metric name}?testRun={true|false}&force={true|false}
`

Examples

Delete one metric

`
curl -X "DELETE" "http://localhost:56787/_dw/rest/carbonj/metrics/pod1x.ecom_ag.100.100_prd.rpc.outgoing.protocols.baskets.get.time.86sum?testRun=false"
`

Delete all metrics in the subtree

`
curl  -X "DELETE" "http://localhost:56787/_dw/rest/carbonj/metrics/pod1x.ecom_ag?force=true&testRun=false"
`





