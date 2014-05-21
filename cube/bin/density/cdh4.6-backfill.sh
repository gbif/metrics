mvn -f ../../pom.xml -Pdev clean assembly:assembly
java -cp .:../../target/classes:../../target/cube-0.14-SNAPSHOT-jar-with-dependencies.jar:$HADOOP_HOME/lib/hadoop-hdfs-2.0.0-cdh4.6.0.jar org.gbif.metrics.cube.tile.density.backfill.Backfill
