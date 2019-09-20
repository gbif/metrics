package org.gbif.metrics.cube.tile.density.backfill;

import org.gbif.metrics.cube.HBaseSourcedBackfill;
import org.gbif.metrics.cube.tile.density.DensityTile;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Runs a backfill to populate the tile densities.
 * TODO: remove this as per http://dev.gbif.org/issues/browse/POR-394
 */
public class Backfill extends HBaseSourcedBackfill {

  private static final Logger LOG = LoggerFactory.getLogger(Backfill.class);

  private final int numZooms;
  private final int pixelsPerCluster;

  // sensible defaults when omitted
  final static int DEFAULT_PIXELS_PER_CLUSTER = 1;
  final static int DEFAULT_NUM_ZOOMS = 4;

  public static final String KEY_NUM_ZOOMS = "density-cube.numZooms";
  public static final String KEY_PIXELS_PER_CLUSTER = "density-cube.tilePixelsPerCluster";

  // the prefix used in the cube.properties
  private static final String PREFIX = "density-cube";

  public Backfill() throws IOException {
    super(PREFIX);
    pixelsPerCluster = PropertiesUtil.propertyAsInt(getProperties(), KEY_PIXELS_PER_CLUSTER, false, DEFAULT_PIXELS_PER_CLUSTER);
    numZooms = PropertiesUtil.propertyAsInt(getProperties(), KEY_NUM_ZOOMS, false, DEFAULT_NUM_ZOOMS);
  }

  public static void main(String[] args) {
    try {
      Backfill app = new Backfill();
      app.backfill(new BackfillCallback(), DensityTile.DensityTileDeserializer.class);
    } catch (Exception e) {
      LOG.error("Error running backfill", e);
    }
  }

  /**
   * Adds more values to the hadoop configuration.
   */
  @Override
  protected void setup(Configuration conf) throws IOException {
    super.setup(conf);
    conf.setInt(KEY_PIXELS_PER_CLUSTER, pixelsPerCluster);
    conf.setInt(KEY_NUM_ZOOMS, numZooms);
  }
}
