package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OccurrenceComparisonUtilTest {

  private static Occurrence buildOccurrence(UUID datasetKey, UUID ook) {
    Occurrence occ1 = new Occurrence();
    occ1.setKey(1);
    occ1.setDatasetKey(datasetKey);
    occ1.setPublishingOrgKey(ook);
    occ1.setBasisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN);
    occ1.setKingdomKey(1);
    occ1.setPhylumKey(1);
    occ1.setClassKey(1);
    occ1.setOrderKey(1);
    occ1.setFamilyKey(1);
    occ1.setGenusKey(1);
    occ1.setSpeciesKey(1);
    occ1.setScientificName("Ursus horribilis");
    occ1.addIssue(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
    occ1.setDecimalLatitude(1.234);
    occ1.setDecimalLongitude(4.567);
    occ1.setCountry(Country.AFGHANISTAN);
    occ1.setPublishingCountry(Country.CANADA);
    occ1.setProtocol(EndpointType.BIOCASE);

    return occ1;
  }

  @Test
  public void testFullEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = buildOccurrence(datasetKey, ook);
    Occurrence occ2 = buildOccurrence(datasetKey, ook);
    assertTrue(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testFullNotEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = buildOccurrence(datasetKey, ook);
    Occurrence occ2 = buildOccurrence(datasetKey, ook);
    occ2.setKey(2);
    assertFalse(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testSomeNullEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = buildOccurrence(datasetKey, ook);
    occ1.setPhylumKey(null);
    occ1.setDecimalLatitude(null);
    Occurrence occ2 = buildOccurrence(datasetKey, ook);
    occ2.setPhylumKey(null);
    occ2.setDecimalLatitude(null);
    assertTrue(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testSomeNullNotEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = buildOccurrence(datasetKey, ook);
    Occurrence occ2 = new Occurrence();
    occ2.setKey(1);
    occ2.setDatasetKey(datasetKey);
    assertFalse(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testAllNull() {
    UUID datasetKey = UUID.randomUUID();
    Occurrence occ1 = new Occurrence();
    occ1.setKey(1);
    occ1.setDatasetKey(datasetKey);
    Occurrence occ2 = new Occurrence();
    occ2.setKey(1);
    occ2.setDatasetKey(datasetKey);
    assertTrue(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }
}
