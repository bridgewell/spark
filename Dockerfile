FROM ubuntu:22.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-8-jdk \
        maven \
        git \
        wget \
        lsb-core \
    && wget https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/packages-microsoft-prod.deb -O /tmp/packages-microsoft-prod.deb \
    && dpkg -i /tmp/packages-microsoft-prod.deb \
    && rm /tmp/packages-microsoft-prod.deb \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y dotnet-sdk-8.0 \
    && wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop2.tgz -O /tmp/spark-3.3.2-bin-hadoop2.tgz \
    && tar -xvzf /tmp/spark-3.3.2-bin-hadoop2.tgz -C /usr/local/bin \
    && rm -rf /tmp/spark-3.3.2-bin-hadoop2.tgz \
    && ln -s /usr/local/bin/spark-3.3.2-bin-hadoop2 /usr/local/bin/spark

ENV SPARK_HOME /usr/local/bin/spark
