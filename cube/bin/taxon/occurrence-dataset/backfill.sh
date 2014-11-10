##
# builds and runs the backfill
#
# Requires that the $HADOOP_HOME be set which is ONLY used for locating the HDFS jar
#   export HADOOP_HOME=/Users/tim/dev/hadoop/dev/cdh5.2.0/hadoop-2.5.0-cdh5.2.0/share/hadoop
#
##
mvn -f ../../../pom.xml -Pdev clean assembly:assembly
java -cp $HADOOP_HOME/hdfs/hadoop-hdfs-2.5.0-cdh5.2.0.jar:./:../../../target/classes:../../../target/cube-0.17-SNAPSHOT-jar-with-dependencies.jar org.gbif.metrics.cube.index.taxon.backfill.Backfill
