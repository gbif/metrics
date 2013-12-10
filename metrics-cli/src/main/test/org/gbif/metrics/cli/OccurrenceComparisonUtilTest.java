package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OccurrenceComparisonUtilTest {

  @Test
  public void testFullEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = Occurrence.builder().key(1).datasetKey(datasetKey)
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .classKey(1)
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .familyKey(1)
      .genusKey(1)
      .geospatialIssue(0)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .latitude(1.234)
      .longitude(4.567)
      .nubKey(1)
      .owningOrgKey(ook)
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    Occurrence occ2 = Occurrence.builder().key(1).datasetKey(datasetKey)
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .classKey(1)
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .familyKey(1)
      .genusKey(1)
      .geospatialIssue(0)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .latitude(1.234)
      .longitude(4.567)
      .nubKey(1)
      .owningOrgKey(ook)
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    assertTrue(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testFullNotEqual() {
    Occurrence occ1 = Occurrence.builder().key(1).datasetKey(UUID.randomUUID())
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .classKey(1)
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .familyKey(1)
      .genusKey(1)
      .geospatialIssue(0)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .latitude(1.234)
      .longitude(4.567)
      .nubKey(1)
      .owningOrgKey(UUID.randomUUID())
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    Occurrence occ2 = Occurrence.builder().key(2).datasetKey(UUID.randomUUID())
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .classKey(1)
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .familyKey(1)
      .genusKey(1)
      .geospatialIssue(0)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .latitude(1.234)
      .longitude(4.567)
      .nubKey(1)
      .owningOrgKey(UUID.randomUUID())
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    assertFalse(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testSomeNullEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = Occurrence.builder().key(1).datasetKey(datasetKey)
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .genusKey(1)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .nubKey(1)
      .owningOrgKey(ook)
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    Occurrence occ2 = Occurrence.builder().key(1).datasetKey(datasetKey)
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .genusKey(1)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .nubKey(1)
      .owningOrgKey(ook)
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    assertTrue(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testSomeNullNotEqual() {
    UUID datasetKey = UUID.randomUUID();
    UUID ook = UUID.randomUUID();
    Occurrence occ1 = Occurrence.builder().key(1).datasetKey(datasetKey)
      .basisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN)
      .catalogNumber("abc")
      .classKey(1)
      .collectionCode("123")
      .country(Country.AFGHANISTAN)
      .familyKey(1)
      .genusKey(1)
      .geospatialIssue(0)
      .hostCountry(Country.CANADA)
      .institutionCode("IC")
      .kingdomKey(1)
      .latitude(1.234)
      .longitude(4.567)
      .nubKey(1)
      .owningOrgKey(ook)
      .phylumKey(1)
      .protocol(EndpointType.BIOCASE)
      .scientificName("Ursus horribilis")
      .speciesKey(1)
      .build();
    Occurrence occ2 = Occurrence.builder().key(1).datasetKey(datasetKey).build();
    assertFalse(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }

  @Test
  public void testAllNull() {
    UUID datasetKey = UUID.randomUUID();
    Occurrence occ1 = Occurrence.builder().key(1).datasetKey(datasetKey).build();
    Occurrence occ2 = Occurrence.builder().key(1).datasetKey(datasetKey).build();
    assertTrue(OccurrenceComparisonUtil.equivalent(occ1, occ2));
  }
}
