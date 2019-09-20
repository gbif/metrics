package org.gbif.metrics.cli;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.metrics.cube.occurrence.OccurrenceAddressUtil;

import java.util.Map.Entry;

import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.ops.LongOp;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Contains unit tests and debug test methods created while diagnosing potential issues.
 */
public class OccurrenceCubeUpdaterServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceCubeUpdaterServiceTest.class);
  private static final ObjectMapper MAPPER;

  static {
    JsonFactory factory = new JsonFactory();
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    MAPPER = new ObjectMapper(factory);
  }


  /**
   * This tests that merges of Batches to update the cube work as advertised.
   */
  @Test
  public void testMerges() {
    try {
      Occurrence oldOccurrence = MAPPER.readValue(
        "{" + "\"key\": 911489332," + "\"datasetKey\": \"b5dc599c-2c51-4878-a92b-52f601f94335\","
        + "\"publishingOrgKey\": \"9898d7f3-bd89-4bc8-87e7-77b60d9f9f18\"," + "\"publishingCountry\": \"BF\","
        + "\"protocol\": \"DWC_ARCHIVE\"," + "\"lastCrawled\": \"2014-06-16T12:29:45.316+0000\","
        + "\"lastParsed\": \"2014-06-16T12:29:45.421+0000\"," + "\"extensions\": {},"
        + "\"basisOfRecord\": \"PRESERVED_SPECIMEN\"," + "\"decimalLongitude\": -5.1852,"
        + "\"decimalLatitude\": 10.3553," + "\"elevation\": 304," + "\"stateProvince\": \"Leraba\"," + "\"year\": 2009,"
        + "\"month\": 5," + "\"day\": 25," + "\"eventDate\": \"2009-05-24T22:00:00.000+0000\"," + "\"issues\": ["
        + "\"GEODETIC_DATUM_ASSUMED_WGS84\"," + "\"PRESUMED_NEGATED_LONGITUDE\"" + "],"
        + "\"lastInterpreted\": \"2014-06-16T12:29:49.580+0000\"," + "\"identifiers\": []," + "\"facts\": [],"
        + "\"relations\": []," + "\"geodeticDatum\": \"WGS84\"," + "\"countryCode\": \"BF\","
        + "\"country\": \"Burkina Faso\","
        + "\"fieldNotes\": \"Superficie Ã©chantillionnÃ©e (m2) : 200 x 200  Nombre de plantes Ã©chantillionnÃ©s : 30  100 plantes touvÃ©es et 20 % de la population produisant des graines.  Les graines ont Ã©tÃ©s rÃ©coltÃ©s des plantes tard en saison Ã  l'Ã©tat sec.\","
        + "\"verbatimLocality\": \"ForÃªt de Lera\"," + "\"institutionID\": \"CNSF\","
        + "\"higherGeography\": \"Burkina Faso, Leraba\"," + "\"occurrenceID\": \"605.0\","
        + "\"ownerInstitutionCode\": \"Centre National des Semences ForestiÃ¨res\","
        + "\"collectionCode\": \"Herbier_CNSF\","
        + "\"occurrenceRemarks\": \", Anthocleista sp, Pavetta, Corymbosa, Alchornea, Rubiaceae, Entada, Albizia etc.., Liane de 6,00 m possedant des Ã©pines Ã  l'aisselle des folioles.\","
        + "\"gbifID\": \"911489332\","
        + "\"habitat\": \"ForÃªt galerie Ã  relief plat, gÃ©ologie hygromorphe, sol noir de structure argileux et Ã  drainage libre.  Facteurs changeant : PiÃ©tinÃ©.\","
        + "\"institutionCode\": \"Centre National des Semences ForestiÃ¨res\"," + "\"catalogNumber\": \"605.0\","
        + "\"recordedBy\": \"Lassina SANOU\"," + "\"otherCatalogNumbers\": \"492\","
        + "\"associatedTaxa\": \"Anthocleista sp, Pavetta, Corymbosa, Alchornea, Rubiaceae, Entada, Albizia etc..\","
        + "\"identifier\": \"605.0\"" + "}", Occurrence.class);

      Occurrence newOccurrence = MAPPER.readValue(
        "{" + "\"key\": 911489332," + "\"datasetKey\": \"b5dc599c-2c51-4878-a92b-52f601f94335\","
        + "\"publishingOrgKey\": null," + "\"publishingCountry\": null," + "\"protocol\": null,"
        + "\"lastCrawled\": null," + "\"lastParsed\": null," + "\"extensions\": {}," + "\"basisOfRecord\": null,"
        + "\"decimalLongitude\": null," + "\"decimalLatitude\": null," + "\"elevation\": null,"
        + "\"stateProvince\": \"null\"," + "\"year\": null," + "\"month\": null," + "\"day\": null,"
        + "\"eventDate\": null," + "\"issues\": []," + "\"lastInterpreted\": null," + "\"identifiers\": [],"
        + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": null," + "\"countryCode\": null,"
        + "\"country\": null," + "\"fieldNotes\": null," + "\"verbatimLocality\": null,"
        + "\"institutionID\": \"CNSF\"," + "\"higherGeography\": null," + "\"occurrenceID\": \"605.0\","
        + "\"ownerInstitutionCode\": \"Centre National des Semences ForestiÃ¨res\","
        + "\"collectionCode\": \"Herbier_CNSF\"," + "\"occurrenceRemarks\": null," + "\"gbifID\": \"911489332\","
        + "\"habitat\": null," + "\"institutionCode\": null," + "\"catalogNumber\": \"605.0\","
        + "\"recordedBy\": null," + "\"otherCatalogNumbers\": \"492\"," + "\"associatedTaxa\": null,"
        + "\"identifier\": \"605.0\"" + "}", Occurrence.class);

      Batch<LongOp> updateNew = OccurrenceAddressUtil.cubeMutation(newOccurrence, new LongOp(1));
      logBatch(updateNew);
      assertIncrementSize(updateNew, 1L);

      Batch<LongOp> updateOld = OccurrenceAddressUtil.cubeMutation(oldOccurrence, new LongOp(-1));
      logBatch(updateOld);
      assertIncrementSize(updateOld, -1L);

      // adding +1 to -1 should make zeros across the board
      updateOld.putAll(OccurrenceAddressUtil.cubeMutation(oldOccurrence, new LongOp(1)));
      logBatch(updateOld);
      assertIncrementSize(updateOld, 0L);

      // and when trimmed that should be empty
      assertTrue(OccurrenceCubeUpdaterService.trimZeros(updateOld).getMap().isEmpty());

    } catch (Exception e) {
      LOG.error("Unexpected exception testing merge", e);
      fail(e.getMessage());
    }
  }

  // pulls 5 sample messages and inspecting them visually for updates
  public void debugBatch() throws Exception {
    Occurrence[] oldOccurrences = new Occurrence[] {
      MAPPER.readValue(
      "{" + "\"key\": 307428507," + "\"datasetKey\": \"086a644d-6cbe-43b7-b7c7-d33d36028d7f\","
      + "\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"publishingCountry\": \"GB\","
      + "\"protocol\": \"DWC_ARCHIVE\"," + "\"lastCrawled\": \"2013-09-07T07:13:38.000+0000\"," + "\"extensions\": {},"
      + "\"basisOfRecord\": \"OBSERVATION\"," + "\"taxonKey\": 2951984," + "\"kingdomKey\": 6," + "\"phylumKey\": 49,"
      + "\"classKey\": 220," + "\"orderKey\": 1370," + "\"familyKey\": 5386," + "\"genusKey\": 2951953,"
      + "\"speciesKey\": 2951984," + "\"scientificName\": \"Ulex europaeus L.\"," + "\"kingdom\": \"Plantae\","
      + "\"phylum\": \"Magnoliophyta\"," + "\"order\": \"Fabales\"," + "\"family\": \"Fabaceae\","
      + "\"genus\": \"Ulex\"," + "\"species\": \"Ulex europaeus\"," + "\"genericName\": \"Ulex\","
      + "\"specificEpithet\": \"europaeus\"," + "\"taxonRank\": \"SPECIES\"," + "\"decimalLongitude\": -2.51001,"
      + "\"decimalLatitude\": 52.0119," + "\"continent\": \"EUROPE\"," + "\"year\": 1999," + "\"issues\": ["
      + "\"COORDINATE_ROUNDED\"," + "\"GEODETIC_DATUM_ASSUMED_WGS84\"" + "],"
      + "\"lastInterpreted\": \"2014-06-05T05:29:31.496+0000\"," + "\"identifiers\": []," + "\"facts\": [],"
      + "\"relations\": []," + "\"geodeticDatum\": \"WGS84\"," + "\"class\": \"Magnoliopsida\","
      + "\"countryCode\": \"GB\"," + "\"country\": \"United Kingdom\"," + "\"gbifID\": \"307428507\","
      + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60288444\","
      + "\"locality\": \"Site name protected\"," + "\"collectionCode\": \"6340\"" + "}", Occurrence.class),

      MAPPER.readValue("{" + "\"key\": 307428296," + "\"datasetKey\": \"086a644d-6cbe-43b7-b7c7-d33d36028d7f\","
                 + "\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"publishingCountry\": \"GB\","
                 + "\"protocol\": \"DWC_ARCHIVE\"," + "\"lastCrawled\": \"2013-09-07T07:13:38.000+0000\","
                 + "\"extensions\": {}," + "\"basisOfRecord\": \"OBSERVATION\"," + "\"taxonKey\": 5275103,"
                 + "\"kingdomKey\": 6," + "\"phylumKey\": 59," + "\"classKey\": 7228684," + "\"orderKey\": 392,"
                 + "\"familyKey\": 2373," + "\"genusKey\": 2651126," + "\"speciesKey\": 5275102,"
                 + "\"scientificName\": \"Dryopteris dilatata auct. non (Hoffm.) A. Gray\","
                 + "\"kingdom\": \"Plantae\"," + "\"phylum\": \"Pteridophyta\"," + "\"order\": \"Polypodiales\","
                 + "\"family\": \"Dryopteridaceae\"," + "\"genus\": \"Dryopteris\","
                 + "\"species\": \"Dryopteris expansa\"," + "\"genericName\": \"Dryopteris\","
                 + "\"specificEpithet\": \"dilatata\"," + "\"taxonRank\": \"SPECIES\","
                 + "\"decimalLongitude\": -2.80792," + "\"decimalLatitude\": 52.3699," + "\"continent\": \"EUROPE\","
                 + "\"year\": 1999," + "\"issues\": [" + "\"COORDINATE_ROUNDED\"," + "\"GEODETIC_DATUM_ASSUMED_WGS84\""
                 + "]," + "\"lastInterpreted\": \"2014-06-05T05:29:30.620+0000\"," + "\"identifiers\": [],"
                 + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": \"WGS84\","
                 + "\"class\": \"Polypodiopsida\"," + "\"countryCode\": \"GB\"," + "\"country\": \"United Kingdom\","
                 + "\"gbifID\": \"307428296\"," + "\"institutionCode\": \"Botanical Society of the British Isles\","
                 + "\"catalogNumber\": \"60288233\"," + "\"locality\": \"Site name protected\","
                 + "\"collectionCode\": \"6340\"" + "}", Occurrence.class),

      MAPPER.readValue(
      "{" + "\"key\": 307428506," + "\"datasetKey\": \"086a644d-6cbe-43b7-b7c7-d33d36028d7f\","
      + "\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"publishingCountry\": \"GB\","
      + "\"protocol\": \"DWC_ARCHIVE\"," + "\"lastCrawled\": \"2013-09-07T07:13:38.000+0000\"," + "\"extensions\": {},"
      + "\"basisOfRecord\": \"OBSERVATION\"," + "\"taxonKey\": 2951984," + "\"kingdomKey\": 6," + "\"phylumKey\": 49,"
      + "\"classKey\": 220," + "\"orderKey\": 1370," + "\"familyKey\": 5386," + "\"genusKey\": 2951953,"
      + "\"speciesKey\": 2951984," + "\"scientificName\": \"Ulex europaeus L.\"," + "\"kingdom\": \"Plantae\","
      + "\"phylum\": \"Magnoliophyta\"," + "\"order\": \"Fabales\"," + "\"family\": \"Fabaceae\","
      + "\"genus\": \"Ulex\"," + "\"species\": \"Ulex europaeus\"," + "\"genericName\": \"Ulex\","
      + "\"specificEpithet\": \"europaeus\"," + "\"taxonRank\": \"SPECIES\"," + "\"decimalLongitude\": -2.65837,"
      + "\"decimalLatitude\": 52.191," + "\"continent\": \"EUROPE\"," + "\"year\": 1999," + "\"issues\": ["
      + "\"COORDINATE_ROUNDED\"," + "\"GEODETIC_DATUM_ASSUMED_WGS84\"" + "],"
      + "\"lastInterpreted\": \"2014-06-05T05:29:31.447+0000\"," + "\"identifiers\": []," + "\"facts\": [],"
      + "\"relations\": []," + "\"geodeticDatum\": \"WGS84\"," + "\"class\": \"Magnoliopsida\","
      + "\"countryCode\": \"GB\"," + "\"country\": \"United Kingdom\"," + "\"gbifID\": \"307428506\","
      + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60288443\","
      + "\"locality\": \"Site name protected\"," + "\"collectionCode\": \"6340\"" + "}", Occurrence.class),

      MAPPER.readValue("{" + "\"key\": 307428485," + "\"datasetKey\": \"086a644d-6cbe-43b7-b7c7-d33d36028d7f\","
                 + "\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"publishingCountry\": \"GB\","
                 + "\"protocol\": \"DWC_ARCHIVE\"," + "\"lastCrawled\": \"2013-09-07T07:13:38.000+0000\","
                 + "\"extensions\": {}," + "\"basisOfRecord\": \"OBSERVATION\"," + "\"taxonKey\": 2888763,"
                 + "\"kingdomKey\": 6," + "\"phylumKey\": 49," + "\"classKey\": 220," + "\"orderKey\": 946,"
                 + "\"familyKey\": 2508," + "\"genusKey\": 2888741," + "\"speciesKey\": 2888763,"
                 + "\"scientificName\": \"Valeriana officinalis L.\"," + "\"kingdom\": \"Plantae\","
                 + "\"phylum\": \"Magnoliophyta\"," + "\"order\": \"Dipsacales\"," + "\"family\": \"Valerianaceae\","
                 + "\"genus\": \"Valeriana\"," + "\"species\": \"Valeriana officinalis\","
                 + "\"genericName\": \"Valeriana\"," + "\"specificEpithet\": \"officinalis\","
                 + "\"taxonRank\": \"SPECIES\"," + "\"decimalLongitude\": -2.79982," + "\"decimalLatitude\": 51.92037,"
                 + "\"continent\": \"EUROPE\"," + "\"year\": 1999," + "\"issues\": [" + "\"COORDINATE_ROUNDED\","
                 + "\"GEODETIC_DATUM_ASSUMED_WGS84\"" + "]," + "\"lastInterpreted\": \"2014-06-05T05:29:30.823+0000\","
                 + "\"identifiers\": []," + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": \"WGS84\","
                 + "\"class\": \"Magnoliopsida\"," + "\"countryCode\": \"GB\"," + "\"country\": \"United Kingdom\","
                 + "\"gbifID\": \"307428485\"," + "\"institutionCode\": \"Botanical Society of the British Isles\","
                 + "\"catalogNumber\": \"60288422\"," + "\"locality\": \"Site name protected\","
                 + "\"collectionCode\": \"6340\"" + "}", Occurrence.class),

      MAPPER.readValue(
      "{" + "\"key\": 307427915," + "\"datasetKey\": \"086a644d-6cbe-43b7-b7c7-d33d36028d7f\","
      + "\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"publishingCountry\": \"GB\","
      + "\"protocol\": \"DWC_ARCHIVE\"," + "\"lastCrawled\": \"2013-09-07T07:13:38.000+0000\"," + "\"extensions\": {},"
      + "\"basisOfRecord\": \"OBSERVATION\"," + "\"taxonKey\": 3105754," + "\"kingdomKey\": 6," + "\"phylumKey\": 49,"
      + "\"classKey\": 220," + "\"orderKey\": 414," + "\"familyKey\": 3065," + "\"genusKey\": 3105646,"
      + "\"speciesKey\": 3105782," + "\"scientificName\": \"Sonchus oleraceus Wall.\"," + "\"kingdom\": \"Plantae\","
      + "\"phylum\": \"Magnoliophyta\"," + "\"order\": \"Asterales\"," + "\"family\": \"Asteraceae\","
      + "\"genus\": \"Sonchus\"," + "\"species\": \"Sonchus asper\"," + "\"genericName\": \"Sonchus\","
      + "\"specificEpithet\": \"oleraceus\"," + "\"taxonRank\": \"SPECIES\"," + "\"decimalLongitude\": -2.80628,"
      + "\"decimalLatitude\": 52.28," + "\"continent\": \"EUROPE\"," + "\"year\": 1999," + "\"issues\": ["
      + "\"COORDINATE_ROUNDED\"," + "\"GEODETIC_DATUM_ASSUMED_WGS84\"" + "],"
      + "\"lastInterpreted\": \"2014-06-05T05:29:30.734+0000\"," + "\"identifiers\": []," + "\"facts\": [],"
      + "\"relations\": []," + "\"geodeticDatum\": \"WGS84\"," + "\"class\": \"Magnoliopsida\","
      + "\"countryCode\": \"GB\"," + "\"country\": \"United Kingdom\"," + "\"gbifID\": \"307427915\","
      + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60287852\","
      + "\"locality\": \"Site name protected\"," + "\"collectionCode\": \"6340\"" + "}", Occurrence.class)};


    Occurrence[] newOccurrences = new Occurrence[] {

      MAPPER.readValue(
      "{\"key\":307428507,\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\","
      + "\"gbifID\": \"307427915\"," + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60287852\",\"collectionCode\": \"6340\","
      + "\"publishingCountry\": null,"
      + "\"protocol\": null," + "\"lastCrawled\": null," + "\"extensions\": {},"
      + "\"basisOfRecord\": null," + "\"taxonKey\": null," + "\"kingdomKey\": null," + "\"phylumKey\": null," + "\"classKey\": null," + "\"orderKey\": null," + "\"familyKey\": null," + "\"genusKey\": null,"
      + "\"speciesKey\": null," + "\"scientificName\": null," + "\"kingdom\": null," + "\"phylum\": null," + "\"order\": null," + "\"family\": null,"
      + "\"genus\": null," + "\"species\": null," + "\"genericName\": null," + "\"specificEpithet\": null," + "\"taxonRank\": null," + "\"decimalLongitude\": null,"
      + "\"decimalLatitude\": null," + "\"continent\": null," + "\"year\": null," + "\"issues\": [],"
      + "\"lastInterpreted\": null," + "\"identifiers\": []," + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": null," + "\"class\": null,"
      + "\"countryCode\": null," + "\"country\": null," + "\"locality\": null}", Occurrence.class),

      MAPPER.readValue("{\"key\":307428296,\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"gbifID\": \"307427915\","
                       + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60287852\",\"collectionCode\": \"6340\","
                       + "\"publishingCountry\": null," + "\"protocol\": null," + "\"lastCrawled\": null," + "\"extensions\": {},"
                       + "\"basisOfRecord\": null," + "\"taxonKey\": null," + "\"kingdomKey\": null," + "\"phylumKey\": null," + "\"classKey\": null,"
                       + "\"orderKey\": null," + "\"familyKey\": null," + "\"genusKey\": null," + "\"speciesKey\": null," + "\"scientificName\": null," + "\"kingdom\": null,"
                       + "\"phylum\": null," + "\"order\": null," + "\"family\": null," + "\"genus\": null," + "\"species\": null," + "\"genericName\": null," + "\"specificEpithet\": null,"
                       + "\"taxonRank\": null," + "\"decimalLongitude\": null," + "\"decimalLatitude\": null," + "\"continent\": null," + "\"year\": null," + "\"issues\": [],"
                       + "\"lastInterpreted\": null," + "\"identifiers\": []," + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": null,"
                       + "\"class\": null," + "\"countryCode\": null," + "\"country\": null," + "\"locality\": null}",
        Occurrence.class),

      MAPPER.readValue("{\"key\":307428506,\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"gbifID\": \"307427915\","
                       + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60287852\",\"collectionCode\": \"6340\","
                       + "\"publishingCountry\": null," + "\"protocol\": null," + "\"lastCrawled\": null," + "\"extensions\": {},"
                       + "\"basisOfRecord\": null," + "\"taxonKey\": null," + "\"kingdomKey\": null," + "\"phylumKey\": null," + "\"classKey\": null,"
                       + "\"orderKey\": null," + "\"familyKey\": null," + "\"genusKey\": null," + "\"speciesKey\": null," + "\"scientificName\": null," + "\"kingdom\": null,"
                       + "\"phylum\": null," + "\"order\": null," + "\"family\": null," + "\"genus\": null," + "\"species\": null," + "\"genericName\": null," + "\"specificEpithet\": null,"
                       + "\"taxonRank\": null," + "\"decimalLongitude\": null," + "\"decimalLatitude\": null," + "\"continent\": null," + "\"year\": null," + "\"issues\": [],"
                       + "\"lastInterpreted\": null," + "\"identifiers\": []," + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": null,"
                       + "\"class\": null," + "\"countryCode\": null," + "\"country\": null," + "\"locality\": null}",
        Occurrence.class),

      MAPPER.readValue("{\"key\":307428485,\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"gbifID\": \"307427915\","
                       + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60287852\",\"collectionCode\": \"6340\","
                       + "\"publishingCountry\": null," + "\"protocol\": null," + "\"lastCrawled\": null," + "\"extensions\": {},"
                       + "\"basisOfRecord\": null," + "\"taxonKey\": null," + "\"kingdomKey\": null," + "\"phylumKey\": null," + "\"classKey\": null,"
                       + "\"orderKey\": null," + "\"familyKey\": null," + "\"genusKey\": null," + "\"speciesKey\": null," + "\"scientificName\": null," + "\"kingdom\": null,"
                       + "\"phylum\": null," + "\"order\": null," + "\"family\": null," + "\"genus\": null," + "\"species\": null," + "\"genericName\": null," + "\"specificEpithet\": null,"
                       + "\"taxonRank\": null," + "\"decimalLongitude\": null," + "\"decimalLatitude\": null," + "\"continent\": null," + "\"year\": null," + "\"issues\": [],"
                       + "\"lastInterpreted\": null," + "\"identifiers\": []," + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": null,"
                       + "\"class\": null," + "\"countryCode\": null," + "\"country\": null," + "\"locality\": null}",
        Occurrence.class),

      MAPPER.readValue("{\"key\":307427915,\"publishingOrgKey\": \"07f617d0-c688-11d8-bf62-b8a03c50a862\"," + "\"gbifID\": \"307427915\","
                       + "\"institutionCode\": \"Botanical Society of the British Isles\"," + "\"catalogNumber\": \"60287852\",\"collectionCode\": \"6340\","
                       + "\"publishingCountry\": null," + "\"protocol\": null," + "\"lastCrawled\": null," + "\"extensions\": {},"
                       + "\"basisOfRecord\": null," + "\"taxonKey\": null," + "\"kingdomKey\": null," + "\"phylumKey\": null," + "\"classKey\": null,"
                       + "\"orderKey\": null," + "\"familyKey\": null," + "\"genusKey\": null," + "\"speciesKey\": null," + "\"scientificName\": null," + "\"kingdom\": null,"
                       + "\"phylum\": null," + "\"order\": null," + "\"family\": null," + "\"genus\": null," + "\"species\": null," + "\"genericName\": null," + "\"specificEpithet\": null,"
                       + "\"taxonRank\": null," + "\"decimalLongitude\": null," + "\"decimalLatitude\": null," + "\"continent\": null," + "\"year\": null," + "\"issues\": [],"
                       + "\"lastInterpreted\": null," + "\"identifiers\": []," + "\"facts\": []," + "\"relations\": []," + "\"geodeticDatum\": null,"
                       + "\"class\": null," + "\"countryCode\": null," + "\"country\": null," + "\"locality\": null}",
        Occurrence.class),
       };

    Batch<LongOp> batch = new Batch<LongOp>();
    for (int i = 0; i < oldOccurrences.length; i++) {
      batch.putAll(OccurrenceAddressUtil.cubeMutation(oldOccurrences[i], new LongOp(-1)));
      batch.putAll(OccurrenceAddressUtil.cubeMutation(newOccurrences[i], new LongOp(1)));
    }
    batch = OccurrenceCubeUpdaterService.trimZeros(batch);

    logBatch(batch);
  }


  private void assertIncrementSize(Batch<LongOp> batch, long amount) {
    for (Entry<Address, LongOp> e : batch.getMap().entrySet()) {
      assertEquals("Batches hold wrong increment amounts", amount, e.getValue().getLong());
    }
  }

  private void logBatch(Batch<LongOp> batch) {
    LOG.debug("Batch of {} entries:", batch.getMap().size());
    for (Entry<Address, LongOp> e : batch.getMap().entrySet()) {
      LOG.info("  {}: {}", e.getKey(), e.getValue());
    }
  }
}
