package org.gbif.metrics.cube.tile.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.hadoop.io.WritableComparable;

/**
 * A container object intended for use as MR values to encapsulate a latitude
 * and longitude pair.
 */
public class LatLngWritable implements WritableComparable<LatLngWritable> {

  private double lat, lng;
  private int count;

  public LatLngWritable() {
  }

  public LatLngWritable(double lat, double lng, int count) {
    this.lat = lat;
    this.lng = lng;
    this.count = count;
  }

  @Override
  public int compareTo(LatLngWritable o) {
    return this.toString().compareTo(o.toString());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LatLngWritable) {
      LatLngWritable that = (LatLngWritable) object;
      return Objects.equal(this.lat, that.lat) && Objects.equal(this.lng, that.lng) && Objects.equal(this.count, that.count);
    }
    return false;
  }

  public int getCount() {
    return count;
  }

  public double getLat() {
    return lat;
  }

  public double getLng() {
    return lng;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(lat, lng, count);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    lat = in.readDouble();
    lng = in.readDouble();
    count = in.readInt();
  }

  public void setCount(int count) {
    this.count = count;
  }

  public void setLat(double lat) {
    this.lat = lat;
  }

  public void setLng(double lng) {
    this.lng = lng;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("lat", lat).add("lng", lng).add("count", count).toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(lat);
    out.writeDouble(lng);
    out.writeInt(count);
  }
}
