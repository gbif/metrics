#!/bin/bash
##
# builds and runs the backfill
#
# With a properly configured environment (e.g. on the gateways) this will work. If your paths aren't quite right and
# you get Hadoop or HDFS errors you need to add $HADOOP_HOME/hadoop-hdfs/*:$HADOOP_HOME/hadoop-mapreduce/* to the cp,
# with HADOOP_HOME set to something like /opt/cloudera/parcels/CDH/lib
#
# Run as ./backfill.sh -nobuild to skip the build step (but note the jar has to be in the /target dir already)
#
##

if [[ hdfs dfs -ls -d /tmp/backfill_snapshot_hfiles ]]; then
	echo >&2 "Directory /tmp/backfill_snapshot_hfiles exists in HDFS, cube build would fail"
	echo >&2 "or maybe you are trying to build two cubes at once — this won't work."
fi

if [ "$1" != "-nobuild" ]
then
  mvn -f ../../pom.xml clean package
fi
cp ../../target/cube-*-for-backfills.jar .
rm -Rf logs
mkdir -m 777 logs
sudo -u hdfs java -cp ../conf:* org.gbif.metrics.cube.occurrence.backfill.Backfill
rm -f cube*backfills.jar
