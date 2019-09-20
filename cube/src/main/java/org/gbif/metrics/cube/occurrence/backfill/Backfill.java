package org.gbif.metrics.cube.occurrence.backfill;

import org.gbif.metrics.cube.HBaseSourcedBackfill;

import java.io.IOException;

import com.urbanairship.datacube.ops.LongOp.LongOpDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs a backfill to populate the tile densities.
 * TODO: remove this as per http://dev.gbif.org/issues/browse/POR-394
 */
public class Backfill extends HBaseSourcedBackfill {

  // the prefix used in the cube.properties
  private static final String PREFIX = "occurrence";

  private static final Logger LOG = LoggerFactory.getLogger(Backfill.class);

  public Backfill() throws IllegalArgumentException, IOException {
    super(PREFIX);
  }

  public static void main(String[] args) {
    try {
      Backfill app = new Backfill();
      app.backfill(new BackfillCallback(), LongOpDeserializer.class);
    } catch (Exception e) {
      LOG.error("Error running backfill", e);
    }
  }
}
