package org.gbif.metrics.cube.tile.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import org.apache.hadoop.io.WritableComparable;

/**
 * A container object intended for use as MR keys to encapsulate everything needed to identify
 * a tile.
 */
public class TileKeyWritable implements WritableComparable<TileKeyWritable> {

  private TileContentType contentType;
  private String key;
  private int x, y, z;

  public TileKeyWritable() {
  }

  public TileKeyWritable(TileContentType type, String key, int x, int y, int z) {
    this.contentType = type;
    this.key = key;
    this.x = x;
    this.y = y;
    this.z = z;
  }

  @Override
  public int compareTo(TileKeyWritable o) {
    return this.toString().compareTo(o.toString());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object instanceof TileKeyWritable) {
      TileKeyWritable that = (TileKeyWritable) object;
      return Objects.equal(this.contentType, that.contentType) && Objects.equal(this.key, that.key) && Objects.equal(this.x, that.x)
        && Objects.equal(this.y, that.y) && Objects.equal(this.z, that.z);
    }
    return false;
  }

  public String getKey() {
    return key;
  }

  public TileContentType getType() {
    return contentType;
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  public int getZ() {
    return z;
  }

  @Override
  public int hashCode() {
    // Note the getId() since enumeration hashcodes are INCONSISTENT across JVMs
    return Objects.hashCode(contentType.getId(), key, x, y, z);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    contentType = TileContentType.INSTANCE(in.readInt());
    key = in.readUTF();
    x = in.readInt();
    y = in.readInt();
    z = in.readInt();
  }

  public void setKey(String key) {
    this.key = key;
  }


  public void setType(TileContentType type) {
    this.contentType = type;
  }

  public void setX(int x) {
    this.x = x;
  }

  public void setY(int y) {
    this.y = y;
  }

  public void setZ(int z) {
    this.z = z;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("contentType", contentType).add("key", key).add("x", x).add("y", y).add("z", z).toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(contentType.getId());
    out.writeUTF(key);
    out.writeInt(x);
    out.writeInt(y);
    out.writeInt(z);
  }
}
