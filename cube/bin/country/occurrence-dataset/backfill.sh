#!/bin/bash
##
# builds and runs the backfill
#
# With a properly configured environment (e.g. on the gateways) this will work. If your paths aren't quite right and
# you get "No filesystem for HDFS" you need to add $HADOOP_HOME/hadoop-hdfs/hadoop-hdfs-2.6.0-cdh5.4.2.jar to the cp,
# with HADOOP_HOME set to something like /opt/cloudera/parcels/CDH/lib
#
# Run as ./backfill.sh -nobuild to skip the build step (but note the jar has to be in the /target dir already)
#
##
if [ "$1" != "-nobuild" ]
then
  mvn -f ../../../pom.xml clean package
fi
cp ../../../target/cube-*-for-backfills.jar .
java -cp ../../conf:* org.gbif.metrics.cube.index.country.backfill.Backfill
rm -f cube*backfills.jar
