# GBIF Metrics Web Services Client

This project provides the WS client to the metrics web services. Because the web services are read-only, it follows that
this client is read-only. It implements the GBIF Registry API.

## Usage

Example:

```java
Properties props = new Properties();
// set this to the web service URL.  It might be localhost:8080 for local development
props.setProperty("metrics.ws.url", "http://api.gbif.org/v1/");
Injector injector = Guice.createInjector(new MetricsWsClientModule(props));
CubeService cubeService = injector.getInstance(CubeService.class);
long numGeoreferenced = cubeService.get(new ReadBuilder().at(OccurrenceCube.IS_GEOREFERENCED, true));
System.out.println(numGeoreferenced);
````
