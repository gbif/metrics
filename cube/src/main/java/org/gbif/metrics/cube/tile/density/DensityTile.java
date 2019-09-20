package org.gbif.metrics.cube.tile.density;

import org.gbif.metrics.cube.tile.MercatorProjectionUtil;
import org.gbif.metrics.cube.tile.density.io.DensityTileAvro;
import org.gbif.metrics.cube.tile.density.io.DensityTileAvroSerDeUtils;
import org.gbif.metrics.cube.tile.density.io.GridAvro;

import java.awt.Point;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;


/**
 * A density tile clusters counts georeferenced items to a configurable sized cluster of pixels,
 * and to a specific layer. For example, a density tile might contain data to 4px groupings
 * for specimens collected this decade, and also observations made within this decade.
 * For example, one might cluster to 4px by 4px. This class wraps an Avro
 * object which is used for efficient SerDe and implements the CubeIo operation
 * interface to allow it to be used for cube additions.
 */
public class DensityTile implements Op {

  /**
   * Simple builder to help produce DensityTiles from lat lng data.
   * Requires the
   */
  public static class Builder {

    private final int x, y, zoom, clusterSize;
    private final Map<Layer, Map<Integer, Integer>> layers = Maps.newHashMap();

    public Builder(int zoom, int x, int y, int clusterSize) {
      this.x = x;
      this.y = y;
      this.zoom = zoom;
      this.clusterSize = clusterSize;
    }

    public DensityTile build() {
      return new DensityTile(x,y,zoom,clusterSize, layers);
    }

    public Builder collect(Layer layer, double lat, double lng, int count) {
      // ignore zero which means no content
      if (count != 0) {
        // assert that we are on the correct tile
        Point t = MercatorProjectionUtil.toTileXY(lat, lng, zoom);
        if (t.x == x && t.y == y) {
          int offsetX = MercatorProjectionUtil.getOffsetX(lat, lng, zoom);
          int offsetY = MercatorProjectionUtil.getOffsetY(lat, lng, zoom);
          int id = toCellId(offsetX, offsetY, clusterSize);

          // create the grid if necessary
          if (!layers.containsKey(layer)) {
            Map<Integer, Integer> g = Maps.newHashMap();
            layers.put(layer, g);
          }
          Map<Integer, Integer> cells = layers.get(layer);

          if (cells.containsKey(id)) {
            cells.put(id, cells.get(id) + count);
          } else {
            cells.put(id, count);
          }
        }
      }
      return this;
    }

    public int getX() {
      return x;
    }

    public int getY() {
      return y;
    }

    public int getZoom() {
      return zoom;
    }
  }

  public static class DensityTileDeserializer implements Deserializer<DensityTile> {

    @Override
    public DensityTile fromBytes(byte[] bytes) {
      try {
        return DensityTile.DESERIALIZE(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Unable to deserialize DensityTile: " + e.getMessage(), e);
      }
    }
  }

  public static final int TILE_SIZE = 256;
  public static final DensityTileDeserializer DESERIALIZER = new DensityTileDeserializer();
  public static final Set<Integer> SUPPORTED_RESOLUTIONS = ImmutableSet.of(1,2,4,8,16,32);

  private final Map<Layer, Map<Integer, Integer>> layers = Maps.newHashMap();
  private final int x, y, zoom, clusterSize;


  public DensityTile(int x, int y, int zoom, int clusterSize) {
    this.x = x;
    this.y = y;
    this.zoom = zoom;
    this.clusterSize = clusterSize;
  }

  public DensityTile(int x, int y, int zoom, int clusterSize, Map<Layer, Map<Integer, Integer>> layers) {
    this.x = x;
    this.y = y;
    this.zoom = zoom;
    this.clusterSize = clusterSize;
    this.layers.putAll(layers);
  }

  public static Builder builder(int zoom, int x, int y, int clusterSize) {
    return new Builder(zoom, x, y, clusterSize);
  }

  /**
   * Utility to deserialize an Avro based serialization.
   *
   * @see DensityTileAvro
   */
  public static DensityTile DESERIALIZE(byte[] b) throws IOException {
    DensityTileAvro avro = DensityTileAvroSerDeUtils.decode(b);
    Map<CharSequence, GridAvro> m = avro.getGrids();
    DensityTile dt = new DensityTile(avro.getX(), avro.getY(), avro.getZoom(), avro.getClusterSize());

    // recreate the grid structure
    for (Entry<CharSequence, GridAvro> e : m.entrySet()) {
      Map<Integer, Integer> cells = Maps.newHashMap();
      dt.layers.put(Layer.valueOf(e.getKey().toString()), cells);
      // populate the cells within the grid just created
      for (Entry<CharSequence, Integer> c : e.getValue().getCells().entrySet()) {
        cells.put(Integer.parseInt(c.getKey().toString()), c.getValue());
      }
    }
    return dt;
  }

  /**
   * Based on the cluster size, generates the cell id from the offset X and Y within
   * the tile. This is a linear form of the usual google tile addressing scheme, where
   * the top row goes left to right 0,1,2,3 etc for as many cells as are in the row.
   */
  public static int toCellId(int x, int y, int clusterSize) {
    int tpc = TILE_SIZE / clusterSize;
    return (x / clusterSize) + (tpc * (y / clusterSize));
  }

  @Override
  public Op add(Op otherOp) {
    if (otherOp instanceof DensityTile) {
      DensityTile o = (DensityTile) otherOp;

      if (o.clusterSize != clusterSize) {
        throw new IllegalArgumentException("Cannot merge DensityTiles with different cluster sizes. Supplied "
          + o.clusterSize + " and " + clusterSize);
      }

      // build a new map merging the densities from the provided cells
      Map<Layer, Map<Integer, Integer>> layers = Maps.newHashMap(layers());
      for (Entry<Layer, Map<Integer, Integer>> g : o.layers.entrySet()) {
        // If we don't have it already, copy it in, otherwise merge it in
        if (!layers.containsKey(g.getKey())) {
          layers.put(g.getKey(), g.getValue());
        } else {
          Map<Integer, Integer> cells = layers.get(g.getKey());
          for (Entry<Integer, Integer> c : g.getValue().entrySet()) {
            // merge in the cell counts
            if (cells.containsKey(c.getKey())) {
              int count = cells.get(c.getKey()) + c.getValue();
              // as data is removed, it is feasible that we end up with no content - save space and remove 0 counts
              if (count == 0) {
                cells.remove(c.getKey());
              } else {
                cells.put(c.getKey(), count);
              }

            } else {
              cells.put(c.getKey(), c.getValue());
            }
          }
        }
      }

      return new DensityTile(x,y,zoom,clusterSize, layers);

    } else {
      throw new IllegalArgumentException(
        "Cannot merge DensityTiles when supplied tile is not a density tile.  Supplied: " + otherOp.getClass());
    }
  }

  /**
   * Looks up a cell value using the encoded form, returning 0 for anything not found.
   */
  public int cell(Layer grid, int cellId) {
    Map<Integer, Integer> cells = layers.get(grid);
    if (cells != null) {
      Integer v = cells.get(cellId);
      return (v == null) ? 0 : v;
    }
    return 0;
  }


  /**
   * Looks up a cell value, returning 0 for anything not found.
   */
  public int cell(Layer grid, int x, int y) {
    int cellId = x + (TILE_SIZE / clusterSize * y);
    return cell(grid, cellId);
  }

  /**
   * A utility to downscale from e.g. 1px resolution to 2px or 4px resolutions.
   * @return a new DensityTile or this, if the there is no work to do
   */
  public DensityTile downscale(int newClusterSize) {
    Preconditions.checkArgument(SUPPORTED_RESOLUTIONS.contains(newClusterSize),
      "Supplied resolution [%s] is not supported.  Supported resolutions %s", newClusterSize, SUPPORTED_RESOLUTIONS);
    Preconditions.checkArgument(newClusterSize>=clusterSize,
      "Can only downscale resolutions.  Current resolution[%s] is coarser than that desired[%s]", clusterSize, newClusterSize);

    if (getClusterSize() == newClusterSize) {
      return this; // nothing to do
    } else {
      // to downscale we basically work out the x,y in the current grid system and then project that onto the new one,
      // by working out how many bits difference there are
      int bitShift = 0;
      for (int i=newClusterSize; i>clusterSize; i = i>>1, bitShift++) {
        ;
      }
      int cellsPerRow = TILE_SIZE / clusterSize;
      int newCellsPerRow = TILE_SIZE / newClusterSize;

      // build a new map scaling all the densities from the provided cells
      Map<Layer, Map<Integer, Integer>> newLayers = Maps.newHashMap();
      for (Entry<Layer, Map<Integer, Integer>> g : layers.entrySet()) {

        Map<Integer, Integer> cells = newLayers.get(g.getKey());

        // If we don't have it already, create it
        if (cells == null) {
          cells = Maps.newHashMap();
          newLayers.put(g.getKey(), cells);
        }

        // downscale and merge in
        for (Entry<Integer, Integer> c : g.getValue().entrySet()) {
          // downscale here (written verbosely to aid future versions of me)
          int currentKey = c.getKey();
          int currentX = currentKey % cellsPerRow;
          int currentY = currentKey / cellsPerRow;
          int newX = currentX>>bitShift;
          int newY = currentY>>bitShift;
          int newKey = (newY * newCellsPerRow) + newX;

          // merge in the cell counts
          if (cells.containsKey(newKey)) {
            int count = cells.get(newKey) + c.getValue();
            cells.put(newKey, count);
          } else {
            cells.put(newKey, c.getValue());
          }
        }
      }

      return new DensityTile(x,y,zoom, newClusterSize, newLayers);

    }
  }

  public int getClusterSize() {
    return clusterSize;
  }

  public Map<Layer, Map<Integer, Integer>> layers() {
    return layers;
  }

  /**
   * Serializes using an Avro format.
   *
   * @see DensityTileAvro
   */
  @Override
  public byte[] serialize() {
    DensityTileAvro avro = new DensityTileAvro();
    avro.setClusterSize(clusterSize);
    Map<CharSequence, GridAvro> g = Maps.newHashMap();
    avro.setGrids(g);
    // Avro only supports String typed maps, hence these lines of workaround type casting
    for (Entry<Layer, Map<Integer, Integer>> layer : layers.entrySet()) {
      Map<CharSequence, Integer> m = Maps.newHashMap();
      for (Entry<Integer, Integer> e : layer.getValue().entrySet()) {
        m.put(String.valueOf(e.getKey()), e.getValue());
      }
      g.put(layer.getKey().toString(), GridAvro.newBuilder().setCells(m).build());
    }

    try {
      return DensityTileAvroSerDeUtils.encode(avro);
    } catch (IOException e) {
      // We can't throw a checked exception, so repackage
      throw new RuntimeException("Unable to serialize: " + e);
    }
  }

  @Override
  public Op subtract(Op otherOp) {
    throw new UnsupportedOperationException();
  }
}