package org.gbif.metrics.cube.mapred;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.OccurrenceResultReader;
import org.gbif.occurrencestore.util.BasisOfRecordConverter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.WritableComparable;

/**
 * A container object for use in MR, that encapsulates the fields needed to locate similar records
 * at the same point. Effectively this is analogous to set of GROUP BY fields.
 */
public class OccurrenceWritable implements WritableComparable<OccurrenceWritable> {

  private Integer kingdomID, phylumID, classID, orderID, familyID, genusID, speciesID, taxonID, issues, year, month,
    count;
  private String publishingOrganisationKey, datasetKey, countryIsoCode, hostCountryIsoCode;
  private Double latitude, longitude;
  private BasisOfRecord basisOfRecord;
  private EndpointType protocol;

  private static final int NULL_INT = -1;
  private static final String NULL_STRING = "NULL";
  private static final double NULL_DOUBLE = -999; // invalid for coordinates
  private final static BasisOfRecordConverter borc = new BasisOfRecordConverter();

  public OccurrenceWritable() {
  }

  public OccurrenceWritable(Integer kingdomID, Integer phylumID, Integer classID, Integer orderID, Integer familyID,
    Integer genusID,
    Integer speciesID, Integer taxonID, Integer issues, String publishingOrganisationKey, String datasetKey,
    String countryIsoCode, String hostCountryIsoCode, Double latitude,
    Double longitude, Integer year, Integer month, BasisOfRecord basisOfRecord, EndpointType protocol, Integer count) {
    this.kingdomID = kingdomID;
    this.phylumID = phylumID;
    this.classID = classID;
    this.orderID = orderID;
    this.familyID = familyID;
    this.genusID = genusID;
    this.speciesID = speciesID;
    this.taxonID = taxonID;
    this.issues = issues;
    this.publishingOrganisationKey = publishingOrganisationKey;
    this.datasetKey = datasetKey;
    this.countryIsoCode = countryIsoCode;
    this.hostCountryIsoCode = hostCountryIsoCode;
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
    Integer issues = OccurrenceResultReader.getInteger(row, FieldName.I_GEOSPATIAL_ISSUE);
    Integer kingdomID = OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_ID);
    Integer phylumID = OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_ID);
    Integer classID = OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_ID);
    Integer orderID = OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_ID);
    Integer familyID = OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_ID);
    Integer genusID = OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_ID);
    Integer speciesID = OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_ID);
    // taxon != species (it may be a higher taxon, or might be a subspecies)
    Integer taxonID = OccurrenceResultReader.getInteger(row, FieldName.I_NUB_ID);
    String publishingOrganisationKey = OccurrenceResultReader.getString(row, FieldName.OWNING_ORG_KEY);
    String datasetKey = OccurrenceResultReader.getString(row, FieldName.DATASET_KEY);
    String countryIsoCode = OccurrenceResultReader.getString(row, FieldName.I_ISO_COUNTRY_CODE);
    String hostCountryIsoCode = OccurrenceResultReader.getString(row, FieldName.HOST_COUNTRY);
    Integer year = OccurrenceResultReader.getInteger(row, FieldName.I_YEAR);
    Integer month = OccurrenceResultReader.getInteger(row, FieldName.I_MONTH);
    Integer borAsInt = OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD);
    BasisOfRecord bor = borc.toEnum(borAsInt);
    EndpointType protocol = EndpointType.valueOf(OccurrenceResultReader.getString(row, FieldName.PROTOCOL));
    return new OccurrenceWritable(kingdomID, phylumID, classID, orderID, familyID, genusID, speciesID,
      taxonID, issues, publishingOrganisationKey, datasetKey, countryIsoCode, hostCountryIsoCode, latitude, longitude,
      year, month, bor, protocol, 1);
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
      .compare(this.speciesID, that.speciesID, Ordering.natural().nullsLast())
      .compare(this.taxonID, that.taxonID, Ordering.natural().nullsLast())
      .compare(this.issues, that.issues, Ordering.natural().nullsLast())
      .compare(this.publishingOrganisationKey, that.publishingOrganisationKey, Ordering.natural().nullsLast())
      .compare(this.datasetKey, that.datasetKey, Ordering.natural().nullsLast())
      .compare(this.countryIsoCode, that.countryIsoCode, Ordering.natural().nullsLast())
      .compare(this.hostCountryIsoCode, that.hostCountryIsoCode, Ordering.natural().nullsLast())
      .compare(this.latitude, that.latitude, Ordering.natural().nullsLast())
      .compare(this.longitude, that.longitude, Ordering.natural().nullsLast())
      .compare(this.year, that.year, Ordering.natural().nullsLast())
      .compare(this.month, that.month, Ordering.natural().nullsLast())
      .compare(this.basisOfRecord, that.basisOfRecord, Ordering.natural().nullsLast())
      .compare(this.count, that.count, Ordering.natural().nullsLast())
      .compare(this.protocol, that.protocol, Ordering.natural().nullsLast())
      .result();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OccurrenceWritable) {
      OccurrenceWritable that = (OccurrenceWritable) object;
      return Objects.equal(this.kingdomID, that.kingdomID) && Objects.equal(this.phylumID, that.phylumID)
        && Objects.equal(this.classID, that.classID) && Objects.equal(this.orderID, that.orderID)
        && Objects.equal(this.familyID, that.familyID)
        && Objects.equal(this.genusID, that.genusID) && Objects.equal(this.speciesID, that.speciesID)
        && Objects.equal(this.taxonID, that.taxonID)
        && Objects.equal(this.issues, that.issues) && Objects.equal(this.year, that.year)
        && Objects.equal(this.month, that.month)
        && Objects.equal(this.publishingOrganisationKey, that.publishingOrganisationKey)
        && Objects.equal(this.datasetKey, that.datasetKey)
        && Objects.equal(this.countryIsoCode, that.countryIsoCode)
        && Objects.equal(this.hostCountryIsoCode, that.hostCountryIsoCode)
        && Objects.equal(this.latitude, that.latitude)
        && Objects.equal(this.longitude, that.longitude) && Objects.equal(this.basisOfRecord, that.basisOfRecord)
        && Objects.equal(this.count, that.count)
        && Objects.equal(this.protocol, that.protocol);
    }
    return false;
  }

  public BasisOfRecord getBasisOfRecord() {
    return basisOfRecord;
  }


  public Integer getClassID() {
    return classID;
  }

  public Integer getCount() {
    return count;
  }

  public String getCountryIsoCode() {
    return countryIsoCode;
  }

  public String getDatasetKey() {
    return datasetKey;
  }


  public Integer getFamilyID() {
    return familyID;
  }


  public Integer getGenusID() {
    return genusID;
  }


  public String getHostCountryIsoCode() {
    return hostCountryIsoCode;
  }


  public Integer getIssues() {
    return issues;
  }


  public Integer getKingdomID() {
    return kingdomID;
  }


  public Double getLatitude() {
    return latitude;
  }


  public Double getLongitude() {
    return longitude;
  }


  public Integer getMonth() {
    return month;
  }


  public Integer getOrderID() {
    return orderID;
  }


  public Integer getPhylumID() {
    return phylumID;
  }


  /**
   * @return the protocol
   */
  public EndpointType getProtocol() {
    return protocol;
  }

  public String getPublishingOrganisationKey() {
    return publishingOrganisationKey;
  }

  public Integer getSpeciesID() {
    return speciesID;
  }

  public Integer getTaxonID() {
    return taxonID;
  }

  public Integer getYear() {
    return year;
  }


  @Override
  public int hashCode() {
    return Objects.hashCode(kingdomID, phylumID, classID, orderID, familyID, genusID, speciesID, taxonID, issues, year,
      month, publishingOrganisationKey, datasetKey, countryIsoCode, hostCountryIsoCode, latitude, longitude,
      basisOfRecord, count, protocol);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    kingdomID = readInt(in);
    phylumID = readInt(in);
    classID = readInt(in);
    orderID = readInt(in);
    familyID = readInt(in);
    genusID = readInt(in);
    speciesID = readInt(in);
    taxonID = readInt(in);
    issues = readInt(in);
    publishingOrganisationKey = readString(in);
    datasetKey = readString(in);
    countryIsoCode = readString(in);
    hostCountryIsoCode = readString(in);
    latitude = readDouble(in);
    longitude = readDouble(in);
    year = readInt(in);
    month = readInt(in);
    basisOfRecord = BasisOfRecord.valueOf(readString(in));
    count = readInt(in);
    protocol = EndpointType.valueOf(readString(in));
  }


  public void setBasisOfRecord(BasisOfRecord basisOfRecord) {
    this.basisOfRecord = basisOfRecord;
  }


  public void setClassID(Integer classID) {
    this.classID = classID;
  }


  public void setCount(Integer count) {
    this.count = count;
  }


  public void setCountryIsoCode(String countryIsoCode) {
    this.countryIsoCode = countryIsoCode;
  }


  public void setDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
  }

  public void setFamilyID(Integer familyID) {
    this.familyID = familyID;
  }


  public void setGenusID(Integer genusID) {
    this.genusID = genusID;
  }


  public void setHostCountryIsoCode(String hostCountryIsoCode) {
    this.hostCountryIsoCode = hostCountryIsoCode;
  }

  public void setIssues(Integer issues) {
    this.issues = issues;
  }


  public void setKingdomID(Integer kingdomID) {
    this.kingdomID = kingdomID;
  }

  public void setLatitude(Double latitude) {
    this.latitude = latitude;
  }

  public void setLongitude(Double longitude) {
    this.longitude = longitude;
  }

  public void setMonth(Integer month) {
    this.month = month;
  }

  public void setOrderID(Integer orderID) {
    this.orderID = orderID;
  }

  public void setPhylumID(Integer phylumID) {
    this.phylumID = phylumID;
  }

  /**
   * @param protocol the protocol to set
   */
  public void setProtocol(EndpointType protocol) {
    this.protocol = protocol;
  }

  public void setPublishingOrganisationKey(String publishingOrganisationKey) {
    this.publishingOrganisationKey = publishingOrganisationKey;
  }

  public void setSpeciesID(Integer speciesID) {
    this.speciesID = speciesID;
  }

  public void setTaxonID(Integer taxonID) {
    this.taxonID = taxonID;
  }

  public void setYear(Integer year) {
    this.year = year;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("super", super.toString()).add("kingdomID", kingdomID)
      .add("phylumID", phylumID).add("classID", classID)
      .add("orderID", orderID).add("familyID", familyID).add("genusID", genusID).add("speciesID", speciesID)
      .add("taxonID", taxonID)
      .add("issues", issues).add("year", year).add("month", month)
      .add("publishingOrganisationKey", publishingOrganisationKey)
      .add("datasetKey", datasetKey)
      .add("countryIsoCode", countryIsoCode).add("hostCountryIsoCode", hostCountryIsoCode)
      .add("latitude", latitude).add("longitude", longitude)
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
    write(out, speciesID);
    write(out, taxonID);
    write(out, issues);
    write(out, publishingOrganisationKey);
    write(out, datasetKey);
    write(out, countryIsoCode);
    write(out, hostCountryIsoCode);
    write(out, latitude);
    write(out, longitude);
    write(out, year);
    write(out, month);
    write(out, basisOfRecord.toString());
    write(out, count);
    write(out, protocol.toString());
  }


  private Double readDouble(DataInput in) throws IOException {
    double v = in.readDouble();
    return (v == NULL_DOUBLE) ? null : v;
  }

  private Integer readInt(DataInput in) throws IOException {
    int v = in.readInt();
    return (v == NULL_INT) ? null : v;
  }

  private String readString(DataInput in) throws IOException {
    String v = in.readUTF();
    return (NULL_STRING.equals(v)) ? null : v;
  }

  private void write(DataOutput out, Double d) throws IOException {
    double v = (d == null) ? NULL_DOUBLE : d;
    out.writeDouble(v);
  }

  private void write(DataOutput out, Integer i) throws IOException {
    int v = (i == null) ? NULL_INT : i;
    out.writeInt(v);
  }

  private void write(DataOutput out, String s) throws IOException {
    String v = (s == null || s.length() == 0) ? NULL_STRING : s;
    out.writeUTF(v);
  }
}
