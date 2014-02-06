package org.gbif.metrics.cube.mapred;

import org.gbif.api.model.common.LinneanClassificationKeys;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.TypeStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.WritableComparable;

/**
 * A container object for use in MR, that encapsulates the fields needed to locate similar records
 * at the same point. Effectively this is analogous to set of GROUP BY fields.
 */
public class OccurrenceWritable implements WritableComparable<OccurrenceWritable>, LinneanClassificationKeys {

  private Integer kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, subgenusKey, speciesKey, taxonKey;
  private Integer year, count;
  private UUID pubOrgKey, datasetKey;
  private Double latitude, longitude;
  private Country country, publishingCountry;
  private BasisOfRecord basisOfRecord;
  private EndpointType protocol;
  private TypeStatus typeStatus;
  private Set<OccurrenceIssue> issues = Sets.newHashSet();

  private static final int NULL_INT = -1;
  private static final String NULL_STRING = "NULL";
  private static final Joiner ISSUE_JOINER = Joiner.on(";").skipNulls();
  private static final Splitter ISSUE_SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
  private static final double NULL_DOUBLE = -999; // invalid for coordinates

  public OccurrenceWritable() {
  }
  public OccurrenceWritable(Occurrence occ, Integer cnt) {
    taxonKey = occ.getTaxonKey();
    ClassificationUtils.copyLinneanClassificationKeys(occ, this);
    year = occ.getYear();
    count = cnt;
    pubOrgKey = occ.getPublishingOrgKey();
    datasetKey = occ.getDatasetKey();
    latitude = occ.getLatitude();
    longitude = occ.getLongitude();
    country = occ.getCountry();
    publishingCountry = occ.getPublishingCountry();
    basisOfRecord = occ.getBasisOfRecord();
    protocol = occ.getProtocol();
    issues = occ.getIssues();
  }

  @Override
  public int compareTo(OccurrenceWritable that) {
    return ComparisonChain.start()
      .compare(this.kingdomKey, that.kingdomKey, Ordering.natural().nullsLast())
      .compare(this.phylumKey, that.phylumKey, Ordering.natural().nullsLast())
      .compare(this.classKey, that.classKey, Ordering.natural().nullsLast())
      .compare(this.orderKey, that.orderKey, Ordering.natural().nullsLast())
      .compare(this.familyKey, that.familyKey, Ordering.natural().nullsLast())
      .compare(this.genusKey, that.genusKey, Ordering.natural().nullsLast())
      .compare(this.subgenusKey, that.subgenusKey, Ordering.natural().nullsLast())
      .compare(this.speciesKey, that.speciesKey, Ordering.natural().nullsLast())
      .compare(this.taxonKey, that.taxonKey, Ordering.natural().nullsLast())
      .compare(this.pubOrgKey, that.pubOrgKey, Ordering.natural().nullsLast())
      .compare(this.datasetKey, that.datasetKey, Ordering.natural().nullsLast())
      .compare(this.country, that.country, Ordering.natural().nullsLast())
      .compare(this.publishingCountry, that.publishingCountry, Ordering.natural().nullsLast())
      .compare(this.latitude, that.latitude, Ordering.natural().nullsLast())
      .compare(this.longitude, that.longitude, Ordering.natural().nullsLast())
      .compare(this.year, that.year, Ordering.natural().nullsLast())
      .compare(this.basisOfRecord, that.basisOfRecord, Ordering.natural().nullsLast())
      .compare(this.count, that.count, Ordering.natural().nullsLast())
      .compare(this.protocol, that.protocol, Ordering.natural().nullsLast())
      .compare(this.typeStatus, that.typeStatus, Ordering.natural().nullsLast())
      .compare(this.issues, that.issues, Ordering.<OccurrenceIssue>natural().lexicographical())
      .result();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OccurrenceWritable) {
      OccurrenceWritable that = (OccurrenceWritable) object;
      return Objects.equal(this.kingdomKey, that.kingdomKey)
        && Objects.equal(this.phylumKey, that.phylumKey)
        && Objects.equal(this.classKey, that.classKey)
        && Objects.equal(this.orderKey, that.orderKey)
        && Objects.equal(this.familyKey, that.familyKey)
        && Objects.equal(this.genusKey, that.genusKey)
        && Objects.equal(this.subgenusKey, that.subgenusKey)
        && Objects.equal(this.speciesKey, that.speciesKey)
        && Objects.equal(this.taxonKey, that.taxonKey)
        && Objects.equal(this.issues, that.issues)
        && Objects.equal(this.year, that.year)
        && Objects.equal(this.pubOrgKey, that.pubOrgKey)
        && Objects.equal(this.datasetKey, that.datasetKey)
        && Objects.equal(this.country, that.country)
        && Objects.equal(this.publishingCountry, that.publishingCountry)
        && Objects.equal(this.latitude, that.latitude)
        && Objects.equal(this.longitude, that.longitude)
        && Objects.equal(this.basisOfRecord, that.basisOfRecord)
        && Objects.equal(this.count, that.count)
        && Objects.equal(this.protocol, that.protocol)
        && Objects.equal(this.typeStatus, that.typeStatus);
    }
    return false;
  }

  public boolean hasSpatialIssue() {
    for (OccurrenceIssue rule : OccurrenceIssue.GEOSPATIAL_RULES) {
      if (issues.contains(rule)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, taxonKey, issues,
      year, pubOrgKey, datasetKey, country, publishingCountry, latitude, longitude,
      basisOfRecord, count, protocol, typeStatus);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    kingdomKey = readInt(in);
    phylumKey = readInt(in);
    classKey = readInt(in);
    orderKey = readInt(in);
    familyKey = readInt(in);
    subgenusKey = readInt(in);
    genusKey = readInt(in);
    speciesKey = readInt(in);
    taxonKey = readInt(in);
    issues = readIssueSet(in);
    pubOrgKey = readUuid(in);
    datasetKey = readUuid(in);
    country = readEnum(in, Country.class);
    publishingCountry = readEnum(in, Country.class);
    latitude = readDouble(in);
    longitude = readDouble(in);
    year = readInt(in);
    basisOfRecord = readEnum(in, BasisOfRecord.class);
    count = readInt(in);
    protocol = readEnum(in, EndpointType.class);
    typeStatus = readEnum(in, TypeStatus.class);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("super", super.toString())
      .add("kingdomKey", kingdomKey)
      .add("phylumKey", phylumKey)
      .add("classKey", classKey)
      .add("orderKey", orderKey)
      .add("familyKey", familyKey)
      .add("genusKey", genusKey)
      .add("subgenusKey", subgenusKey)
      .add("speciesKey", speciesKey)
      .add("taxonKey", taxonKey)
      .add("issues", issues)
      .add("year", year)
      .add("pubOrgKey", pubOrgKey)
      .add("datasetKey", datasetKey)
      .add("country", country)
      .add("publishingCountry", publishingCountry)
      .add("latitude", latitude)
      .add("longitude", longitude)
      .add("basisOfRecord", basisOfRecord)
      .add("count", count)
      .add("protocol", protocol)
      .add("typeStatus", typeStatus)
      .toString();
  }


  @Override
  public void write(DataOutput out) throws IOException {
    write(out, kingdomKey);
    write(out, phylumKey);
    write(out, classKey);
    write(out, orderKey);
    write(out, familyKey);
    write(out, genusKey);
    write(out, subgenusKey);
    write(out, speciesKey);
    write(out, taxonKey);
    write(out, issues);
    write(out, pubOrgKey);
    write(out, datasetKey);
    write(out, country);
    write(out, publishingCountry);
    write(out, latitude);
    write(out, longitude);
    write(out, year);
    write(out, basisOfRecord);
    write(out, count);
    write(out, protocol);
    write(out, typeStatus);
  }


  private Double readDouble(DataInput in) throws IOException {
    double v = in.readDouble();
    return (v == NULL_DOUBLE) ? null : v;
  }

  private Integer readInt(DataInput in) throws IOException {
    int v = in.readInt();
    return (v == NULL_INT) ? null : v;
  }

  private boolean readBool(DataInput in) throws IOException {
    return in.readBoolean();
  }

  private String readString(DataInput in) throws IOException {
    String v = in.readUTF();
    return (NULL_STRING.equals(v)) ? null : v;
  }

  private <T extends Enum<?>> T readEnum(DataInput in, Class<T> enumClass) throws IOException {
    int v = in.readInt();
    return (v == NULL_INT) ? null : enumClass.getEnumConstants()[v];
  }

  private static <T extends Enum<?>> T toEnum(String x, Class<T> enumClass) {
    T[] values = enumClass.getEnumConstants();
    for (T val : values) {
      if (x.equals(val.name())) {
        return val;
      }
    }
    return null;
  }

  private UUID readUuid(DataInput in) throws IOException {
    String v = in.readUTF();
    return (NULL_STRING.equals(v)) ? null : toUUID(v);
  }

  private static UUID toUUID(String uuidString) {
    if (!Strings.isNullOrEmpty(uuidString)) {
      try {
        return UUID.fromString(uuidString);
      } catch (IllegalArgumentException e) {
        // swallow
      }
    }
    return null;
  }

  private Set<OccurrenceIssue> readIssueSet(DataInput in) throws IOException {
    Set<OccurrenceIssue> issues = Sets.newHashSet();

    String v = in.readUTF();
    if (!NULL_STRING.equals(v)) {
      Iterator<String> iter = ISSUE_SPLITTER.split(v).iterator();
      while (iter.hasNext()) {
        issues.add(OccurrenceIssue.valueOf(iter.next()));
      }
    }
    return issues;
  }

  private void write(DataOutput out, Double d) throws IOException {
    double v = (d == null) ? NULL_DOUBLE : d;
    out.writeDouble(v);
  }

  private void write(DataOutput out, Integer i) throws IOException {
    int v = (i == null) ? NULL_INT : i;
    out.writeInt(v);
  }

  private void write(DataOutput out, boolean b) throws IOException {
    out.writeBoolean(b);
  }

  private void write(DataOutput out, String s) throws IOException {
    String v = (s == null || s.length() == 0) ? NULL_STRING : s;
    out.writeUTF(v);
  }

  private void write(DataOutput out, Enum<?> e) throws IOException {
    int v = (e == null) ? NULL_INT : e.ordinal();
    out.writeInt(v);
  }

  private void write(DataOutput out, UUID uuid) throws IOException {
    String v = uuid == null ? NULL_STRING : uuid.toString();
    out.writeUTF(v);
  }

  private void write(DataOutput out, Set<? extends Enum> s) throws IOException {
    String v = (s == null || s.size() == 0) ? NULL_STRING : ISSUE_JOINER.join(s);
    out.writeUTF(v);
  }

  public Integer getKingdomKey() {
    return kingdomKey;
  }

  public void setKingdomKey(Integer kingdomKey) {
    this.kingdomKey = kingdomKey;
  }

  public Integer getPhylumKey() {
    return phylumKey;
  }

  public void setPhylumKey(Integer phylumKey) {
    this.phylumKey = phylumKey;
  }

  public Integer getClassKey() {
    return classKey;
  }

  public void setClassKey(Integer classKey) {
    this.classKey = classKey;
  }

  public Integer getOrderKey() {
    return orderKey;
  }

  public void setOrderKey(Integer orderKey) {
    this.orderKey = orderKey;
  }

  public Integer getFamilyKey() {
    return familyKey;
  }

  public void setFamilyKey(Integer familyKey) {
    this.familyKey = familyKey;
  }

  public Integer getGenusKey() {
    return genusKey;
  }

  public void setGenusKey(Integer genusKey) {
    this.genusKey = genusKey;
  }

  public Integer getSubgenusKey() {
    return subgenusKey;
  }

  public void setSubgenusKey(Integer subgenusKey) {
    this.subgenusKey = subgenusKey;
  }

  @Nullable
  @Override
  public Integer getHigherRankKey(Rank rank) {
    return ClassificationUtils.getHigherRankKey(this, rank);
  }

  public Integer getSpeciesKey() {
    return speciesKey;
  }

  public void setSpeciesKey(Integer speciesKey) {
    this.speciesKey = speciesKey;
  }

  public Integer getTaxonKey() {
    return taxonKey;
  }

  public void setTaxonKey(Integer taxonKey) {
    this.taxonKey = taxonKey;
  }

  public Integer getYear() {
    return year;
  }

  public void setYear(Integer year) {
    this.year = year;
  }

  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public UUID getPubOrgKey() {
    return pubOrgKey;
  }

  public void setPubOrgKey(UUID pubOrgKey) {
    this.pubOrgKey = pubOrgKey;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public void setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public Double getLatitude() {
    return latitude;
  }

  public void setLatitude(Double latitude) {
    this.latitude = latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public void setLongitude(Double longitude) {
    this.longitude = longitude;
  }

  public Country getCountry() {
    return country;
  }

  public void setCountry(Country country) {
    this.country = country;
  }

  public Country getPublishingCountry() {
    return publishingCountry;
  }

  public void setPublishingCountry(Country publishingCountry) {
    this.publishingCountry = publishingCountry;
  }

  public BasisOfRecord getBasisOfRecord() {
    return basisOfRecord;
  }

  public void setBasisOfRecord(BasisOfRecord basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }

  public EndpointType getProtocol() {
    return protocol;
  }

  public void setProtocol(EndpointType protocol) {
    this.protocol = protocol;
  }

  public Set<OccurrenceIssue> getIssues() {
    return issues;
  }

  public void setIssues(Set<OccurrenceIssue> issues) {
    this.issues = issues;
  }

  public TypeStatus getTypeStatus() {
    return typeStatus;
  }

  public void setTypeStatus(TypeStatus typeStatus) {
    this.typeStatus = typeStatus;
  }
}
