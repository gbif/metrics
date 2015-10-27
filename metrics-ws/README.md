# GBIF Metrics Web Services

This project exposes read only web services on top of the various cubes.

## Building

Note that any metrics.properties or hadoop/cluster configuration files (e.g. hbase-site.xml) are filtered out of the
final artifact by the shade plugin, with the expectation that the metrics.properties and hbase-site.xml will be provided
in the runtime environment (typically by copying them from the gbif-configuration project).

Build the artifact with:

````shell
mvn clean package
````

## Usage
The artifact created with 'package' can be used together with gbif-microservices to run as jetty within the gbif ws environments (dev, uat, prod).

The provided configuration files in src/main/resources will connect a registry-ws to the dev cluster when started as:

````shell
mvn clean jetty:run
````

To verify, visit:
  http://localhost:8080/occurrence/count
  http://localhost:8080/name_usage/1/occurrence/dataset
