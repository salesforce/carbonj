#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

FROM registry.access.redhat.com/ubi9/ubi:9.4-1214.1726694543
ARG DEPENDENCY=src/main/docker/files
ARG VERSION=latest

# install dependencies
# Install aiohttp required by the script
# Ensure crontab starts
# Add Tini for proper sigkill handling https://github.com/krallin/tini
RUN yum update -y && \
  yum install -y libevent perl wget util-linux-ng cronie procps nc lsof hostname && \
  yum install -y gcc-c++ gcc make libtool automake autoconf make python3-devel && \
  rpm --import http://repos.azulsystems.com/RPM-GPG-KEY-azulsystems && \
  yum install -y https://cdn.azul.com/zulu/bin/zulu-repo-1.0.0-1.noarch.rpm && \
  yum install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/pcp-conf-6.2.1-1.el9.x86_64.rpm && \
  yum install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/pcp-libs-6.2.1-1.el9.x86_64.rpm  && \
  #
  # If sysstat version is updated, confirm iolog.sh execution and update associated version check in entrypoint.sh
  #
  yum install -y https://mirror.stream.centos.org/9-stream/AppStream/x86_64/os/Packages/sysstat-12.5.4-7.el9.x86_64.rpm && \
  yum install -y https://repo.almalinux.org/almalinux/9/extras/x86_64/os/Packages/epel-release-9-5.el9.noarch.rpm && \
  yum install -y https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/Packages/p/perl-URI-Encode-1.1.1-17.el9.noarch.rpm && \
  yum install -y https://rpmfind.net/linux/centos-stream/9-stream/BaseOS/x86_64/os/Packages/snappy-1.1.8-8.el9.x86_64.rpm && \
  yum update -y && \
  yum install -y zulu17-jdk python3 perl-Data-Dumper && \
  yum clean all && \
  dnf update && \
  dnf -y install chkconfig && \
  dnf clean all && \
  pip3 install --upgrade pip && \
  pip3 install aiohttp && \
  chkconfig crond on && \
  systemctl enable crond.service && \
  mkdir -p /app/bin/ && \
  wget https://github.com/krallin/tini/releases/download/v0.19.0/tini && \
  mv tini /sbin/tini && \
  chmod +x /sbin/tini

# add java code
#COPY ${DEPENDENCY}/BOOT-INF/lib /app/lib
#COPY ${DEPENDENCY}/META-INF /app/META-INF
#COPY ${DEPENDENCY}/BOOT-INF/classes /app
COPY build/libs/carbonj.service-${VERSION}.jar /app
# add scripts
COPY ${DEPENDENCY}/entrypoint.sh ${DEPENDENCY}/onOutOfMemoryError.sh ${DEPENDENCY}/logCleanup.sh \
  ${DEPENDENCY}/deletemetrics.py ${DEPENDENCY}/disklog.sh ${DEPENDENCY}/fdlog.sh ${DEPENDENCY}/iolog.sh \
  ${DEPENDENCY}/reportGcMetrics.sh ${DEPENDENCY}/reportRocksDbMetrics.sh ${DEPENDENCY}/requestlog-stats.pl \
  ${DEPENDENCY}/whisper.py ${DEPENDENCY}/cj-load.py ${DEPENDENCY}/check-invalid-namespaces.py \
  ${DEPENDENCY}/delete-invalid-namespaces.py /app/bin/
RUN chmod ugo+x /app/bin/*
# add java options file
# default configs
COPY ${DEPENDENCY}/service.args ${DEPENDENCY}/audit-rules.conf ${DEPENDENCY}/blacklist.conf \
  ${DEPENDENCY}/query-blacklist.conf ${DEPENDENCY}/relay-rules.conf ${DEPENDENCY}/storage-aggregation.conf /app/config/

ENV TERM=xterm
ENV USE_JAVA_ARCH_64=true
ENV SVC_PROP_JETTY_PORT=2001
ENV SVC_PROP_APP_NAME=carbonj
ENV DW_LOG_LEVEL=INFO
ENV DW_SVC_VERSION=${VERSION}
ENV SVC_PROP_DW_SVC_VERSION=${VERSION}
# mertics prefix for requestlog-stats.pl
ENV DW_PREFIX=jetty
ENV SERVICEDIR=/app/
ENV app_servicedir=/app/
ENV ROCKSDB_READONLY=false

ENTRYPOINT ["/sbin/tini", "--"]

# Run your program under Tini
CMD ["/app/bin/entrypoint.sh"]
