#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#

FROM centos:7
ARG DEPENDENCY=target/dependency
ARG VERSION=latest

# install dependencies
RUN yum update -y && \
    yum install -y yum-plugin-ovl
RUN yum update -y && \
  yum install -y \
  sysvinit-tools \
  curl \
  libevent \
  perl \
  tar \
  wget \
  util-linux-ng \
  cronie \
  procps \
  bash \
  gzip \
  nc \
  lsof \
  sysstat \
  epel-release \
  less \
  && wget https://repo.ius.io/ius-release-el7.rpm \
  && rpm -Uvh ius-release*rpm \
  && rm ius-release*rpm \
  && yum install -y   python36u \
    python36u-pip \
    perl-URI-Encode \
    perl-Data-Dumper

RUN rpm --import http://repos.azulsystems.com/RPM-GPG-KEY-azulsystems && \
     curl -o /etc/yum.repos.d/zulu.repo http://repos.azulsystems.com/rhel/zulu.repo && \
     yum update -y && \
     yum install -y zulu-11 && \
     yum clean all

# Install aiohttp required by the script
RUN pip3.6 install aiohttp

# Ensure crontab starts
RUN /sbin/chkconfig crond on && \
  systemctl enable crond.service

# add java code
COPY ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY ${DEPENDENCY}/META-INF /app/META-INF
COPY ${DEPENDENCY}/BOOT-INF/classes /app
# add scripts
RUN mkdir -p /app/bin/
COPY ${DEPENDENCY}/entrypoint.sh /app/bin/
COPY ${DEPENDENCY}/onOutOfMemoryError.sh /app/bin/
COPY ${DEPENDENCY}/logCleanup.sh /app/bin/
COPY ${DEPENDENCY}/deletemetrics.py /app/bin/
COPY ${DEPENDENCY}/disklog.sh /app/bin/
COPY ${DEPENDENCY}/fdlog.sh /app/bin/
COPY ${DEPENDENCY}/iolog.sh /app/bin/
COPY ${DEPENDENCY}/reportGcMetrics.sh /app/bin/
COPY ${DEPENDENCY}/reportRocksDbMetrics.sh /app/bin/
COPY ${DEPENDENCY}/requestlog-stats.pl /app/bin/
COPY ${DEPENDENCY}/whisper.py /app/bin/
COPY ${DEPENDENCY}/cj-load.py /app/bin/
# add java options file
COPY ${DEPENDENCY}/service.args /app/config/
# default configs
COPY ${DEPENDENCY}/audit-rules.conf /app/config/
COPY ${DEPENDENCY}/blacklist.conf /app/config/
COPY ${DEPENDENCY}/query-blacklist.conf /app/config/
COPY ${DEPENDENCY}/relay-rules.conf /app/config/
COPY ${DEPENDENCY}/storage-aggregation.conf /app/config/

RUN chmod ugo+x /app/bin/deletemetrics.py
RUN chmod ugo+x /app/bin/onOutOfMemoryError.sh
RUN chmod ugo+x /app/bin/entrypoint.sh
RUN chmod ugo+x /app/bin/logCleanup.sh
RUN chmod ugo+x /app/bin/disklog.sh
RUN chmod ugo+x /app/bin/fdlog.sh
RUN chmod ugo+x /app/bin/iolog.sh
RUN chmod ugo+x /app/bin/reportGcMetrics.sh
RUN chmod ugo+x /app/bin/reportRocksDbMetrics.sh
RUN chmod ugo+x /app/bin/requestlog-stats.pl

VOLUME /data

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

# Add Tini for proper sigkill handling https://github.com/krallin/tini
ENV TINI_VERSION v0.18.0
RUN curl -o /sbin/tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /sbin/tini
RUN chmod +x /sbin/tini
ENTRYPOINT ["/sbin/tini", "--"]

# Run your program under Tini
CMD ["/app/bin/entrypoint.sh"]
