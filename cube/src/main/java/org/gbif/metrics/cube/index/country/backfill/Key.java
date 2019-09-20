package org.gbif.metrics.cube.index.country.backfill;

import org.gbif.api.vocabulary.Country;

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
  private Country country;

  public Key() {
  }

  public Key(UUID datasetKey, Country country) {
    Preconditions.checkNotNull(datasetKey, "DatasetKey cannot be null");
    this.datasetKey = datasetKey;
    this.country = country;
  }

  @Override
  public int compareTo(Key o) {
    return ComparisonChain.start()
      .compare(country, o.country)
      .compare(datasetKey, o.datasetKey)
      .result();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Key) {
      Key that = (Key) object;
      return Objects.equal(this.datasetKey, that.datasetKey)
        && Objects.equal(this.country, that.country);
    }
    return false;
  }

  public Country getCountry() {
    return country;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datasetKey, country);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    country = Country.fromIsoCode(in.readUTF());
    datasetKey = UUID.fromString(in.readUTF());
  }

  public void setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public void setNubKey(Country country) {
    this.country = country;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("datasetKey", datasetKey)
      .add("country", country)
      .toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(country.getIso2LetterCode());
    out.writeUTF(datasetKey.toString());
  }
}
