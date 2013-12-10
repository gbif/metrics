package org.gbif.metrics.cube.tile.point;

import org.gbif.metrics.cube.tile.MercatorProjectionUtil;
import org.gbif.metrics.cube.tile.io.LocationAvro;
import org.gbif.metrics.cube.tile.io.LocationsAvro;
import org.gbif.metrics.cube.tile.io.LocationsAvroSerDeUtils;

import java.awt.Point;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import org.apache.commons.lang.NotImplementedException;


/**
 * A point tile holds a collection of point locations with an ID at the location.
 */
public class PointTile implements Op {

  /**
   * Simple builder to help produce PointTiles from lat lng data.
   */
  public static class Builder {

    // X, Y, zoom locate the tile
    private final int x, y, zoom;
    private final LocationsAvro locations = new LocationsAvro();

    public Builder(int zoom, int x, int y) {
      this.x = x;
      this.y = y;
      this.zoom = zoom;
    }

    public PointTile build() {
      return new PointTile(locations);
    }

    public Builder collect(double lat, double lng, int id) {
      // assert that we are on the correct tile
      Point t = MercatorProjectionUtil.toTileXY(lat, lng, zoom);
      if (t.x == x && t.y == y) {
        LocationAvro l = new LocationAvro();
        l.setId(id);
        l.setLat(lat);
        l.setLng(lng);
        if (locations.getLocations() == null) {
          List<LocationAvro> loc = Lists.newArrayList();
          locations.setLocations(loc);
        }
        locations.getLocations().add(l);
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

  public static class PointTileDeserializer implements Deserializer<PointTile> {

    @Override
    public PointTile fromBytes(byte[] bytes) {
      try {
        return PointTile.DESERIALIZE(bytes);
      } catch (IOException e) {
        throw new RuntimeException("Unable to deserialize PointTile: " + e.getMessage());
      }
    }
  }

  public static final int TILE_SIZE = 256;
  private LocationsAvro locations;
  public static final PointTileDeserializer DESERIALIZER = new PointTileDeserializer();

  public PointTile() {
  }

  public PointTile(LocationsAvro locations) {
    this.locations = locations;
  }

  public static Builder builder(int zoom, int x, int y) {
    return new Builder(zoom, x, y);
  }

  /**
   * Utility to deserialize an Avro based serialization.
   * 
   * @see LocationsAvro
   */
  public static PointTile DESERIALIZE(byte[] b) throws IOException {
    return new PointTile(LocationsAvroSerDeUtils.decode(b));
  }

  @Override
  public Op add(Op otherOp) {
    if (otherOp instanceof PointTile) {
      PointTile o = (PointTile) otherOp;

      List<LocationAvro> l = Lists.newArrayList();
      if (locations().getLocations() != null) {
        l.addAll(locations().getLocations());
      }
      if (o.locations().getLocations() != null) {
        l.addAll(o.locations().getLocations());
      }
      LocationsAvro ll = new LocationsAvro();
      ll.setLocations(l);
      return new PointTile(ll);

    } else {
      throw new IllegalArgumentException("Cannot merge PointTiles when supplied tile is not a point tile.  Supplied: " + otherOp.getClass());
    }
  }


  public LocationsAvro locations() {
    return locations;
  }

  /**
   * Serializes using an Avro format.
   * 
   * @see LocationAvro
   */
  @Override
  public byte[] serialize() {
    try {
      return LocationsAvroSerDeUtils.encode(locations());
    } catch (IOException e) {
      // We can't throw a checked exception, so repackage
      throw new RuntimeException("Unable to serialize: " + e);
    }
  }

  @Override
  public Op subtract(Op otherOp) {
    throw new NotImplementedException();
  }
}