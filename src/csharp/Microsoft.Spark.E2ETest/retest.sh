#!/bin/bash
set -ex

# rebuild jar
pushd ../../scala/microsoft-spark-3-3
mvn clean package
cp target/microsoft-spark-3-3_2.12-2.1.1.jar /usr/local/dotnet_worker/
rm -rf /tmp/*
popd

# rebuild worker and put to workerdir
pushd ../Microsoft.Spark.Worker
dotnet build
cp -R ../../../artifacts/bin/Microsoft.Spark.Worker/Debug/net8.0/* /usr/local/dotnet_worker/
popd


dotnet test -l "console;verbosity=detailed" -l "html;LogFileName=TestResults.html" --filter "FullyQualifiedName~TestUdfClosure"
