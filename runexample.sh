#!/bin/bash
set -ex

cd src/csharp
dotnet build

cd ../..
export sparkdir=$(pwd)
export testver=3-3
export publish=Debug/net8.0
export DOTNET_WOKER_DIR=${sparkdir}/artifacts/bin/Microsoft.Spark.Worker/${publish}

spark-submit \
--class org.apache.spark.deploy.dotnet.DotnetRunner \
--master local \
${sparkdir}/src/scala/microsoft-spark-${testver}/target/microsoft-spark-${testver}_2.12-2.1.1.jar \
${sparkdir}/artifacts/bin/Microsoft.Spark.CSharp.Examples/${publish}/Microsoft.Spark.CSharp.Examples Sql.Batch.Basic $SPARK_HOME/examples/src/main/resources/people.json