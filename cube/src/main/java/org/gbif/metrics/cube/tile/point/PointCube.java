package org.gbif.metrics.cube.tile.point;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.bucketers.BigEndianIntBucketer;

/**
 * The cube definition for a point based map.
 */
public class PointCube {

  // no id substitution, 4 bytes for int
  public static final Dimension<Integer> TAXON_ID = new Dimension<Integer>("taxonID", new BigEndianIntBucketer(), false, 4);
  public static final Dimension<Integer> ZOOM = new Dimension<Integer>("zoom", new BigEndianIntBucketer(), false, 4);
  public static final Dimension<Integer> TILE_X = new Dimension<Integer>("tileX", new BigEndianIntBucketer(), false, 4);
  public static final Dimension<Integer> TILE_Y = new Dimension<Integer>("tileY", new BigEndianIntBucketer(), false, 4);

  // Singleton instance if accessed through the instance() method
  public static final DataCube<PointTile> INSTANCE = newInstance();

  // Not for instantiation
  private PointCube() {
  }

  private static DataCube<PointTile> newInstance() {
    List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(TAXON_ID, ZOOM, TILE_X, TILE_Y);
    List<Rollup> rollups = ImmutableList.of(new Rollup(TAXON_ID, ZOOM, TILE_X, TILE_Y));
    return new DataCube<PointTile>(dimensions, rollups);
  }
}
