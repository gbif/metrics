package org.gbif.metrics.cube.index.taxon.backfill;

import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.index.common.UuidIntMap.CountByDatasetDeserializer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs a backfill to populate the index.
 * TODO: remove this as per http://dev.gbif.org/issues/browse/POR-394
 */
public class Backfill extends HBaseSourcedBackfill {

  // the prefix used in the cube.properties
  private static final String PREFIX = "taxon.occurrence_dataset";

  private static final Logger LOG = LoggerFactory.getLogger(Backfill.class);

  public Backfill() throws IllegalArgumentException, IOException {
    super(PREFIX);
  }

  public static void main(String[] args) {
    try {
      Backfill app = new Backfill();
      app.backfill(new BackfillCallback(), CountByDatasetDeserializer.class);
    } catch (Exception e) {
      LOG.error("Error running backfill", e);
    }
  }
}
