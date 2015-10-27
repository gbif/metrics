# GBIF Metrics CLI

This provides runnable services that subscribe to occurrence change events, and modify the downstream cubes (which
are occurrence_cube, maps_cube, dataset_country_cube and dataset_taxon_cube).


## Building
To run this, it is expected that you would build the project as a single jar:
````shell
mvn clean package
````

## Usage
And then run it using one or more config files.

Example complete config files are given in the example-conf folder, with placeholders to supply messaging credentials, and the location of the HBase configuration file.

Examples (note you can pass a standard logback xml file in the properties as shown in the second example - this is the usual way of starting this project):

````shell
java -Xmx1G -jar target/metrics-cli.jar DensityCube --conf ./conf/density-updater.yaml
java -Xmx1G -jar target/metrics-cli.jar OccurrenceCube --conf ./conf/occurrence-updater.yaml --log-config ./conf/logback-crawler.xml
````

If you run the application without any parameters, full instructions are given listing the available services.

It should be noted that you can override any property from the configuration file (or omit it) and supply it with the --property-name option.
