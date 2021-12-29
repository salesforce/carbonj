# CarbonJ

CarbonJ is a drop-in replacement for carbon-cache and carbon-relay. It was designed with high performance read and write throughput in mind and supports writing millions of metric data points and serve millions of metrics datapoints per minute with low query latency. CarbonJ is designed to run on medium-size instances (AWS: c5.4xlarge or smaller, 500k-1M/m metrics storage capacity per core) and standard (_no_ provisioned iops) GP2 EBS volumes.

This project seeks to sequentialize the random write I/O typically generated by carbon-cache. Carbon-cache writes 'Whisper'-files for each metric. These files have to be updated in the frequency of arriving metrics data -- for millions of metrics data points per minute carbon-cache needs to write millions of files in random order. On aggregation these files need to be rewritten as well when higher resolution gets rolled into lower resolution, causing even more churn.

CarbonJ instead uses JVM-embedded RocksDB to store all metrics datapoints once per interval and different buckets for different resolutions, avoiding churn on aggregation.

It is currently running the Salesforce CommerceCloud Grafana/Graphite metrics stack and handles ~45M metric data points arriving every minute using 6 shards.

[![](https://github.com/salesforce/carbonj/workflows/Java%20CI/badge.svg)]( https://github.com/salesforce/carbonj/actions?query=workflow%3A%22Java+CI%22)


# Features

## Supported protocols

### Storage
- Line
- Pickle
### Retrieval
- JSON
- Pickle

## Sharding by metric key

## Carbon-Relay functionality

- carbon-relay like metrics routing
- carbon-relay compatible blacklisting
- AWS Kinesis streams: Instead of routing metrics data to CarbonJ, Kinesis streams can be used instead. This allows for loss-free restarts of CarbonJ storage nodes.

https://github.com/salesforce/carbonj/blob/master/CarbonJ.pdf

# How to build

Run `gradle build` to run the test suite and create the Spring boot jar. Run `./gradle build` to run the test suite and create the Spring boot jar. Run
`./gradlew carbonj.service:docker -PdockerRepo="my-docker-repo.com/"` to build the docker image. 

# How to deploy
Provided in this repository is a working helm chart to deploy an instance of CarbonJ.  To deploy it a single partition/shard (numbered 1), you can run helm from the root of this repo with the below command.

`helm install --set carbonj.shard.id=1 carbonj-p1 ./kube/helm/carbonj`
* The `--set carbonj.shard.id=1` option is redundant since this is the default shard id.  The name is given as `carbonj-p1` to match the shard id.
* If the Kubernetes Namespace or StorageClass resource already exists in the cluster, you'll see an exception during the helm install and will need to remove the resource that the exception complains about from the helm chart and attempt the helm install again. 

There are several values that can be tweaked in `./kube/helm/carbonj/values.yaml`.  The values can be set to non-default value by updating that file or by using Helm's `--set <key>=<value>` from the command itself.  Keys with a small description and default values are below.  This current configuration will create 2 replicas of CarbonJ, one in `us-east-1a` and one in `us-east-1b` for high availability.  The memory settings depend on the instance type.  This configuration has proven to be run a metrics stack which a shard/partition persisting around 8 million metrics per minute.

| Key | Description | Default |
|------|-------------|---------|
carbonj.shard.id | Idendification number of shard | 1
carbonj.env | Environment | dev
carbonj.namespace | Kubernetes namespace to deploy to | carbonj
carbonj.version | Version of CarbonJ to deploy | 1.1.17
carbonj.nodes | Number of replicas to run per shard | 2
carbonj.instanceType | AWS instance type to deploy to | r5.2xlarge
carbonj.registry | Container registry to pull image from | chrbayer84
carbonj.memory.xms | Initial Java heap size (-Xms value) | 2G
carbonj.memory.xmx | Maximum Java heap size (-Xmx value) | 18G
carbonj.spring.profiles.active | Active Spring Profile to use | dev
carbonj.groupId | Group ID - used in the metrics emmitted from CarbonJ | dev
carbonj.requests.memory | Kubernetes memory request | 3G
carbonj.requests.cpu | Kubernetes CPU request | 6
carbonj.limits.memory | Kubernetes memory limit | 20G
carbonj.limits.cpu | Kubernetes CPU limit | 8
carbonj.diskSize | AWS EBS Volume size | 3Ti
carbonj.storageClass | AWS EBS Volume storage class | gp2
carbonj.region | AWS region to deploy to | us-east-1
carbonj.availabilityZone1 | AWS availability zone to deploy replica 0 to | a
carbonj.availabilityZone2 | AWS availability zone to deploy replica 1 to | b
