package org.gbif.metrics.cube.tile.density;

import org.gbif.metrics.cube.tile.bucketer.MapKeyBucketer;
import org.gbif.metrics.cube.tile.io.TileContentType;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.bucketers.BigEndianIntBucketer;
import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;

/**
 * The cube definition for a density map.
 */
public class DensityCube {

  public static final Dimension<TileContentType> TYPE = new Dimension<TileContentType>("type", new EnumToOrdinalBucketer<TileContentType>(1), false,
    1);
  public static final Dimension<String> KEY = new Dimension<String>("key", new MapKeyBucketer(), false, MapKeyBucketer.BYTES);
  public static final Dimension<Integer> ZOOM = new Dimension<Integer>("zoom", new BigEndianIntBucketer(), false, 4);
  public static final Dimension<Integer> TILE_X = new Dimension<Integer>("tileX", new BigEndianIntBucketer(), false, 4);
  public static final Dimension<Integer> TILE_Y = new Dimension<Integer>("tileY", new BigEndianIntBucketer(), false, 4);

  // Singleton instance if accessed through the instance() method
  public static final DataCube<DensityTile> INSTANCE = newInstance();

  // Not for instantiation
  private DensityCube() {
  }

  private static DataCube<DensityTile> newInstance() {
    List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(TYPE, KEY, ZOOM, TILE_X, TILE_Y);
    List<Rollup> rollups = ImmutableList.of(new Rollup(TYPE, KEY, ZOOM, TILE_X, TILE_Y));
    return new DataCube<DensityTile>(dimensions, rollups);
  }
}
