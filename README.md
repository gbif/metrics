# GBIF Metrics Project

_The cubes have been replaced by ElasticSearch facets, and the webservice here has been updated to query ES._

The GBIF occurrence metrics. These provide the counts throughout the portal (e.g. gbif.org/occurrence).

The Metrics project includes:
  1. [metrics-es](metrics-es/README.md): Elastic Search metrics
  2. [metrics-ws](metrics-ws/README.md): A metrics ws project to expose read only WS on the ES facets
  3. [metrics-ws-client](metrics-ws-client/README.md): Java client to the WS

## Building
See the individual sub-module READMEs for specific details, but in general it is enough to build all components with:

````shell
mvn clean package
````
