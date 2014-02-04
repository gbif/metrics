metrics
=======

The GBIF cube metrics and maps. It is heavily based on the Urban Airship Datacube:
https://github.com/urbanairship/datacube

The metrics projects include:
1) cube: Cube definitions and utilities to perform backfills (batch population) on them
2) metrics-updater: A service to listen to messages and update cubes in real time (to be renamed cube-updater in the future)
3) metrics-ws: A metrics ws project to expose read only WS on the cubes
4) metrics-ws-client: Java client to the WS