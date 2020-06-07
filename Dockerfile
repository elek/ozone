FROM flokkr/base:36
RUN alternatives --set java java-latest-openjdk.x86_64
#async profiler for development profiling
RUN mkdir -p /opt/profiler && \
    cd /opt/profiler && \
    curl -L https://github.com/jvm-profiling-tools/async-profiler/releases/download/v1.5/async-profiler-1.5-linux-x64.tar.gz | tar xvz
ENV ASYNC_PROFILER_HOME=/opt/profiler

#byteman
RUN yum install -y unzip
RUN curl -s https://gist.githubusercontent.com/elek/23b8b8c5f2849572d4d3b0751076eb21/raw/77e2cae6d360a459c285affb605b52fd3d4cfb17/bitman.sh | bash

COPY hadoop-ozone/dist/target/ozone-0.6.0-SNAPSHOT /opt/ozone

ADD dev-support/byteman/*.btm /tmp/

ENV CONF_DIR /opt/ozone/etc/hadoop
ENV PATH $PATH:/opt/ozone/bin
WORKDIR /opt/ozone
