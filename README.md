# GBIF Metrics Project

The GBIF cube metrics and maps. These provide the counts throughout the portal (e.g. gbif.org/occurrence) and all map data. 
Map tiles are served by the tile-server from these same cubes, but it accesses them directly (ie not through this WS). 
It is heavily based on the Urban Airship Datacube: https://github.com/urbanairship/datacube

The Metrics project include:
  1. cube: Cube definitions and utilities to perform backfills (batch population) on them
  2. metrics-cli: A service to listen to messages and update cubes in real time
  3. metrics-ws: A metrics ws project to expose read only WS on the cubes
  4. metrics-ws-client: Java client to the WS

## Building
See the individual sub-module READMEs for specific details, but in general it is enough to build all components with:

````shell
mvn clean package
````
