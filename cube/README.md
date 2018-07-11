# GBIF Metrics Cube

This project holds the cube definitions, guice modules and classes to perform IO on the cube, and backfill processes
to populate the cubes in a batch mode.

## Usage

The backfill scripts are most easily run from one of the cluster gateway machines. Inside bin/* you will find shell scripts
and links to properties files for the 3 cubes in use: occurrence (occurrence_cube), country (dataset_country_cube) and taxon (dataset_taxon_cube).

To run a backfill:
  1. ensure your hadoop configuration in the bin/conf folder is correct for the environment you are running (see the README in bin/conf)
  2. modify the cube.properties that you are backfilling, observing the comments in the files
  3. delete the old hbase table (disable / drop in hbase shell)
     a. Or any any case, make sure you are building to a new HBase table
     b. Also make sure /tmp/backfill_snapshot_hfiles and /tmp/snapshot_hfiles don't exist in HBase
  4. create a screen session or use `nohup` with output redirect (the process needs to keep running even if the ssh connection is cut)
  5. choose
```shell
./backfill.sh
```
to first build the cube jar and then run your backfill job, or
```shell
./backfill.sh -nobuild
```
if the jar has already been built.
