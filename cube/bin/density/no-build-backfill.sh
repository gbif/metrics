##
# Runs the backfill
#
# Requires that the $HADOOP_HOME be set which is ONLY used for locating the HDFS jar
#   export HADOOP_HOME=/Users/tim/dev/hadoop/dev/cdh5.2.0/hadoop-2.5.0-cdh5.2.0/share/hadoop
#
##
java -cp $HADOOP_HOME/hadoop-hdfs/hadoop-hdfs-2.6.0-cdh5.4.2.jar:./:../../target/classes:../../target/cube-0.23-SNAPSHOT-jar-with-dependencies.jar org.gbif.metrics.cube.tile.density.backfill.Backfill
