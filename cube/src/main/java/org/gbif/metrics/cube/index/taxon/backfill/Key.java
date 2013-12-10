package org.gbif.metrics.cube.index.taxon.backfill;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

/**
 * Synthetic key to use as an intermediary between M and R.
 */
public class Key implements WritableComparable<Key> {

  private UUID datasetKey;
  private int nubKey;

  public Key() {
  }

  public Key(UUID datasetKey, int nubKey) {
    Preconditions.checkNotNull(datasetKey, "DatasetKey cannot be null");
    this.datasetKey = datasetKey;
    this.nubKey = nubKey;
  }

  @Override
  public int compareTo(Key o) {
    return ComparisonChain.start()
      .compare(nubKey, o.nubKey)
      .compare(datasetKey, o.datasetKey)
      .result();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Key) {
      Key that = (Key) object;
      return Objects.equal(this.datasetKey, that.datasetKey)
        && Objects.equal(this.nubKey, that.nubKey);
    }
    return false;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public int getNubKey() {
    return nubKey;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datasetKey, nubKey);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    nubKey = in.readInt();
    datasetKey = UUID.fromString(in.readUTF());
  }

  public void setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public void setNubKey(int nubKey) {
    this.nubKey = nubKey;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("datasetKey", datasetKey)
      .add("nubKey", nubKey)
      .toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(nubKey);
    out.writeUTF(datasetKey.toString());
  }
}
