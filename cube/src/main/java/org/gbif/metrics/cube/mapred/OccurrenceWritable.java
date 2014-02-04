package org.gbif.metrics.cube.mapred;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.OccurrenceResultReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.WritableComparable;

/**
 * A container object for use in MR, that encapsulates the fields needed to locate similar records
 * at the same point. Effectively this is analogous to set of GROUP BY fields.
 */
public class OccurrenceWritable implements WritableComparable<OccurrenceWritable> {

  private Integer kingdomID, phylumID, classID, orderID, familyID, genusID, subgenusID, speciesID, taxonID, year, month, count;
  private UUID publishingOrganisationKey, datasetKey;
  private Double latitude, longitude;
  private Country country, publishingCountry;
  private BasisOfRecord basisOfRecord;
  private EndpointType protocol;
  private Set<OccurrenceIssue> issues = Sets.newHashSet();

  private static final int NULL_INT = -1;
  private static final String NULL_STRING = "NULL";
  private static final Joiner ISSUE_JOINER = Joiner.on(";").skipNulls();
  private static final Splitter ISSUE_SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
  private static final double NULL_DOUBLE = -999; // invalid for coordinates

  public OccurrenceWritable() {
  }

  public OccurrenceWritable(Integer kingdomID, Integer phylumID, Integer classID, Integer orderID, Integer familyID,
    Integer genusID, Integer subgenusID,
    Integer speciesID, Integer taxonID, Set<OccurrenceIssue> issues,
    UUID publishingOrganisationKey, UUID datasetKey,
    Country country, Country publishingCountry, Double latitude,
    Double longitude, Integer year, Integer month, BasisOfRecord basisOfRecord, EndpointType protocol, Integer count) {
    this.kingdomID = kingdomID;
    this.phylumID = phylumID;
    this.classID = classID;
    this.orderID = orderID;
    this.familyID = familyID;
    this.genusID = genusID;
    this.subgenusID = subgenusID;
    this.speciesID = speciesID;
    this.taxonID = taxonID;
    this.issues = issues;
    this.publishingOrganisationKey = publishingOrganisationKey;
    this.datasetKey = datasetKey;
    this.country = country;
    this.publishingCountry = publishingCountry;
    this.latitude = latitude;
    this.longitude = longitude;
    this.year = year;
    this.month = month;
    this.basisOfRecord = basisOfRecord;
    this.count = count;
    this.protocol = protocol;
  }

  // Utility builder
  public static OccurrenceWritable newInstance(Result row) {
    Double latitude = OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE);
    Double longitude = OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE);
    Integer kingdomID = OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_KEY);
    Integer phylumID = OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_KEY);
    Integer classID = OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_KEY);
    Integer orderID = OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_KEY);
    Integer familyID = OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_KEY);
    Integer genusID = OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_KEY);
    Integer subgenusID = OccurrenceResultReader.getInteger(row, FieldName.I_SUBGENUS_KEY);
    Integer speciesID = OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_KEY);
    // taxon != species (it may be a higher taxon, or might be a subspecies)
    Integer taxonID = OccurrenceResultReader.getInteger(row, FieldName.I_TAXON_KEY);
    UUID publishingOrganisationKey = toUUID(row, FieldName.PUB_ORG_KEY);
    UUID datasetKey = toUUID(row, FieldName.DATASET_KEY);
    Country country = toEnum(OccurrenceResultReader.getString(row, FieldName.I_COUNTRY), Country.class);
    Country pubCountry = toEnum(OccurrenceResultReader.getString(row, FieldName.PUB_COUNTRY), Country.class);
    Integer year = OccurrenceResultReader.getInteger(row, FieldName.I_YEAR);
    Integer month = OccurrenceResultReader.getInteger(row, FieldName.I_MONTH);
    BasisOfRecord bor = BasisOfRecord.valueOf(OccurrenceResultReader.getString(row, FieldName.I_BASIS_OF_RECORD));
    EndpointType protocol = EndpointType.valueOf(OccurrenceResultReader.getString(row, FieldName.PROTOCOL));

    //TODO: read issues from hbase row
    Set<OccurrenceIssue> issues = Sets.newHashSet();

    return new OccurrenceWritable(kingdomID, phylumID, classID, orderID, familyID, genusID, subgenusID, speciesID,
      taxonID, issues, publishingOrganisationKey, datasetKey, country, pubCountry, latitude, longitude,
      year, month, bor, protocol, 1);
  }

  private static UUID toUUID(Result row, FieldName field) {
    return toUUID(OccurrenceResultReader.getString(row, field));
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

    @Override
  public int compareTo(OccurrenceWritable that) {
    return ComparisonChain.start()
      .compare(this.kingdomID, that.kingdomID, Ordering.natural().nullsLast())
      .compare(this.phylumID, that.phylumID, Ordering.natural().nullsLast())
      .compare(this.classID, that.classID, Ordering.natural().nullsLast())
      .compare(this.orderID, that.orderID, Ordering.natural().nullsLast())
      .compare(this.familyID, that.familyID, Ordering.natural().nullsLast())
      .compare(this.genusID, that.genusID, Ordering.natural().nullsLast())
      .compare(this.subgenusID, that.subgenusID, Ordering.natural().nullsLast())
      .compare(this.speciesID, that.speciesID, Ordering.natural().nullsLast())
      .compare(this.taxonID, that.taxonID, Ordering.natural().nullsLast())
      .compare(this.publishingOrganisationKey, that.publishingOrganisationKey, Ordering.natural().nullsLast())
      .compare(this.datasetKey, that.datasetKey, Ordering.natural().nullsLast())
      .compare(this.country, that.country, Ordering.natural().nullsLast())
      .compare(this.publishingCountry, that.publishingCountry, Ordering.natural().nullsLast())
      .compare(this.latitude, that.latitude, Ordering.natural().nullsLast())
      .compare(this.longitude, that.longitude, Ordering.natural().nullsLast())
      .compare(this.year, that.year, Ordering.natural().nullsLast())
      .compare(this.month, that.month, Ordering.natural().nullsLast())
      .compare(this.basisOfRecord, that.basisOfRecord, Ordering.natural().nullsLast())
      .compare(this.count, that.count, Ordering.natural().nullsLast())
      .compare(this.protocol, that.protocol, Ordering.natural().nullsLast())
      .compare(this.issues, that.issues, Ordering.<OccurrenceIssue>natural().lexicographical())
      .result();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OccurrenceWritable) {
      OccurrenceWritable that = (OccurrenceWritable) object;
      return Objects.equal(this.kingdomID, that.kingdomID)
        && Objects.equal(this.phylumID, that.phylumID)
        && Objects.equal(this.classID, that.classID)
        && Objects.equal(this.orderID, that.orderID)
        && Objects.equal(this.familyID, that.familyID)
        && Objects.equal(this.genusID, that.genusID)
        && Objects.equal(this.subgenusID, that.subgenusID)
        && Objects.equal(this.speciesID, that.speciesID)
        && Objects.equal(this.taxonID, that.taxonID)
        && Objects.equal(this.issues, that.issues)
        && Objects.equal(this.year, that.year)
        && Objects.equal(this.month, that.month)
        && Objects.equal(this.publishingOrganisationKey, that.publishingOrganisationKey)
        && Objects.equal(this.datasetKey, that.datasetKey)
        && Objects.equal(this.country, that.country)
        && Objects.equal(this.publishingCountry, that.publishingCountry)
        && Objects.equal(this.latitude, that.latitude)
        && Objects.equal(this.longitude, that.longitude)
        && Objects.equal(this.basisOfRecord, that.basisOfRecord)
        && Objects.equal(this.count, that.count)
        && Objects.equal(this.protocol, that.protocol);
    }
    return false;
  }

  public static boolean hasSpatialIssue(Set<OccurrenceIssue> issues) {
    for (OccurrenceIssue rule : OccurrenceIssue.GEOSPATIAL_RULES) {
      if (issues.contains(rule)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(kingdomID, phylumID, classID, orderID, familyID, genusID, speciesID, taxonID, issues,
      year, month, publishingOrganisationKey, datasetKey, country, publishingCountry, latitude, longitude,
      basisOfRecord, count, protocol);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    kingdomID = readInt(in);
    phylumID = readInt(in);
    classID = readInt(in);
    orderID = readInt(in);
    familyID = readInt(in);
    subgenusID = readInt(in);
    genusID = readInt(in);
    speciesID = readInt(in);
    taxonID = readInt(in);
    issues = readIssueSet(in);
    publishingOrganisationKey = readUuid(in);
    datasetKey = readUuid(in);
    country = readEnum(in, Country.class);
    publishingCountry = readEnum(in, Country.class);
    latitude = readDouble(in);
    longitude = readDouble(in);
    year = readInt(in);
    month = readInt(in);
    basisOfRecord = readEnum(in, BasisOfRecord.class);
    count = readInt(in);
    protocol = readEnum(in, EndpointType.class);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("super", super.toString())
      .add("kingdomID", kingdomID)
      .add("phylumID", phylumID)
      .add("classID", classID)
      .add("orderID", orderID)
      .add("familyID", familyID)
      .add("genusID", genusID)
      .add("subgenusID", subgenusID)
      .add("speciesID", speciesID)
      .add("taxonID", taxonID)
      .add("issues", issues)
      .add("year", year)
      .add("month", month)
      .add("publishingOrganisationKey", publishingOrganisationKey)
      .add("datasetKey", datasetKey)
      .add("country", country)
      .add("publishingCountry", publishingCountry)
      .add("latitude", latitude)
      .add("longitude", longitude)
      .add("basisOfRecord", basisOfRecord)
      .add("count", count)
      .add("protocol", protocol).toString();
  }


  @Override
  public void write(DataOutput out) throws IOException {
    write(out, kingdomID);
    write(out, phylumID);
    write(out, classID);
    write(out, orderID);
    write(out, familyID);
    write(out, genusID);
    write(out, subgenusID);
    write(out, speciesID);
    write(out, taxonID);
    write(out, issues);
    write(out, publishingOrganisationKey);
    write(out, datasetKey);
    write(out, country);
    write(out, publishingCountry);
    write(out, latitude);
    write(out, longitude);
    write(out, year);
    write(out, month);
    write(out, basisOfRecord);
    write(out, count);
    write(out, protocol);
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

  public Integer getKingdomID() {
    return kingdomID;
  }

  public void setKingdomID(Integer kingdomID) {
    this.kingdomID = kingdomID;
  }

  public Integer getPhylumID() {
    return phylumID;
  }

  public void setPhylumID(Integer phylumID) {
    this.phylumID = phylumID;
  }

  public Integer getClassID() {
    return classID;
  }

  public void setClassID(Integer classID) {
    this.classID = classID;
  }

  public Integer getOrderID() {
    return orderID;
  }

  public void setOrderID(Integer orderID) {
    this.orderID = orderID;
  }

  public Integer getFamilyID() {
    return familyID;
  }

  public void setFamilyID(Integer familyID) {
    this.familyID = familyID;
  }

  public Integer getGenusID() {
    return genusID;
  }

  public void setGenusID(Integer genusID) {
    this.genusID = genusID;
  }

  public Integer getSubgenusID() {
    return subgenusID;
  }

  public void setSubgenusID(Integer subgenusID) {
    this.subgenusID = subgenusID;
  }

  public Integer getSpeciesID() {
    return speciesID;
  }

  public void setSpeciesID(Integer speciesID) {
    this.speciesID = speciesID;
  }

  public Integer getTaxonID() {
    return taxonID;
  }

  public void setTaxonID(Integer taxonID) {
    this.taxonID = taxonID;
  }

  public Integer getYear() {
    return year;
  }

  public void setYear(Integer year) {
    this.year = year;
  }

  public Integer getMonth() {
    return month;
  }

  public void setMonth(Integer month) {
    this.month = month;
  }

  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public UUID getPublishingOrganisationKey() {
    return publishingOrganisationKey;
  }

  public void setPublishingOrganisationKey(UUID publishingOrganisationKey) {
    this.publishingOrganisationKey = publishingOrganisationKey;
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
}
