#!/bin/bash

# go to project root.
cd ../

# build the maven artifact.
mvn clean install -DskipTests

# copy dependencies
rm -rf additional-jars
mvn dependency:copy-dependencies -DoutputDirectory=additional-jars  -DexcludeTransitive=true
cp -v --no-clobber additional-jars/* $SPARK_HOME/jars
rm -rf additional-jars

# remove previous jars.
rm -f $SPARK_HOME/jars/spark-msgpack*.jar

# copy jar to $SPARK_HOME
cp -v ./target/spark-msgpack*.jar $SPARK_HOME/jars