# GBIF Metrics Web Services

This project exposes read only web services on top of the ElasticSearch metrics.

## Building

Note that any metrics.properties or logback configuration files are filtered out of the
final artifact by the shade plugin, with the expectation that the metrics.properties and logback.xml will be provided
in the runtime environment (typically by copying them from the gbif-configuration project).

Build the artifact with:

````shell
mvn clean package
````

## Usage
The artifact created with 'package' can be used together with gbif-microservices to run as jetty within the gbif ws environments (dev, uat, prod).

The provided configuration files in src/main/resources will connect a registry-ws to the dev cluster when started as:

````shell
mvn clean -Pdev jetty:run
````
where the 'dev' profile needs to look similar to:

````xml
  <profile>
    <id>dev</id>
    <properties>
      <metrics.cache.expire_after>360000</metrics.cache.expire_after>
      <metrics.es.index_name>occurrence</metrics.es.index_name>
      <metrics.es.hosts>http://es.gbif.org:9200</metrics.es.hosts>
    </properties>
  </profile>
````

To verify, visit:
  - http://localhost:8080/occurrence/count
