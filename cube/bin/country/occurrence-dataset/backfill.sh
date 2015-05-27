##
# builds and runs the backfill
#
# Requires that the $HADOOP_HOME be set which is ONLY used for locating the HDFS jar
#   export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib
#
##
mvn -f ../../../pom.xml clean assembly:assembly
java -cp $HADOOP_HOME/hadoop-hdfs/hadoop-hdfs-2.6.0-cdh5.4.2.jar:./:../../../target/classes:../../../target/cube-0.23-SNAPSHOT-jar-with-dependencies.jar org.gbif.metrics.cube.index.country.backfill.Backfill

