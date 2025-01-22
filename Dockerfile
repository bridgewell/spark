FROM harbor.local.bridgewell.com/docker.io/ubuntu:20.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-8-jdk \
        maven \
        git \
        wget \
    && wget https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh \
    && chmod +x /tmp/dotnet-install.sh \
    && /tmp/dotnet-install.sh -i /usr/local/bin \
    && wget https://archive.apache.org/dist/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz -O /tmp/spark-2.3.2-bin-hadoop2.7.tgz \
    && tar -xvzf /tmp/spark-2.3.2-bin-hadoop2.7.tgz -C /usr/local/ \
    && rm -rf /tmp/spark-2.3.2-bin-hadoop2.7.tgz \
    && rm -rf /tmp/dotnet-install.sh
    && ln -s /usr/local/bin/spark /usr/local/bin/spark-2.3.2-bin-hadoop2.7

ENV SPARK_HOME /usr/local/bin/spark
