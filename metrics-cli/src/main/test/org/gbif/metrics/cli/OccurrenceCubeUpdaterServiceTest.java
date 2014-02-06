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
      Occurrence oldOccurrence = MAPPER.readValue("{\"key\":307075245,\"kingdom\":\"Plantae\",\"phylum\":\"Magnoliophyta\",\"clazz\":\"Magnoliopsida\",\"order\":\"Lamiales\",\"family\":\"Lamiaceae\",\"genus\":\"Stachys\",\"subgenus\":null,\"species\":\"Stachys palustris\",\"kingdomKey\":6,\"phylumKey\":49,\"classKey\":220,\"orderKey\":408,\"familyKey\":2497,\"genusKey\":2927228,\"subgenusKey\":null,\"speciesKey\":2927245,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"59935182\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Stachys palustris L.\",\"nubKey\":2927245,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-5.54387,\"latitude\":55.28758,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"No site name available\",\"county\":null,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":1997,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":0,\"otherIssue\":0,\"unitQualifier\":null,\"modified\":1368692609000,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":\"GB\",\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class);
      Occurrence newOccurrence = MAPPER.readValue("{\"key\":307075245,\"kingdom\":null,\"phylum\":null,\"clazz\":null,\"order\":null,\"family\":null,\"genus\":null,\"subgenus\":null,\"species\":null,\"kingdomKey\":null,\"phylumKey\":null,\"classKey\":null,\"orderKey\":null,\"familyKey\":null,\"genusKey\":null,\"subgenusKey\":null,\"speciesKey\":null,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"59935182\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":null,\"scientificName\":null,\"nubKey\":null,\"basisOfRecord\":\"OBSERVATION\",\"longitude\":null,\"latitude\":null,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"No site name available\",\"county\":null,\"stateProvince\":null,\"country\":null,\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":null,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":null,\"otherIssue\":null,\"unitQualifier\":null,\"modified\":1378376618291,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":null,\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class);
      
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
      MAPPER.readValue("{\"key\":307428507,\"kingdom\":\"Plantae\",\"phylum\":\"Magnoliophyta\",\"clazz\":\"Magnoliopsida\",\"order\":\"Fabales\",\"family\":\"Fabaceae\",\"genus\":\"Ulex\",\"subgenus\":null,\"species\":\"Ulex europaeus\",\"kingdomKey\":6,\"phylumKey\":49,\"classKey\":220,\"orderKey\":1370,\"familyKey\":5386,\"genusKey\":2951953,\"subgenusKey\":null,\"speciesKey\":2951984,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288444\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Ulex europaeus L.\",\"nubKey\":2951984,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-2.51001,\"latitude\":52.0119,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":1999,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":0,\"otherIssue\":0,\"unitQualifier\":null,\"modified\":1368692634000,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":\"GB\",\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class), 
      MAPPER.readValue("{\"key\":307428296,\"kingdom\":\"Plantae\",\"phylum\":\"Pteridophyta\",\"clazz\":\"Polypodiopsida\",\"order\":\"Polypodiales\",\"family\":\"Dryopteridaceae\",\"genus\":\"Dryopteris\",\"subgenus\":null,\"species\":\"Dryopteris expansa\",\"kingdomKey\":6,\"phylumKey\":59,\"classKey\":7228684,\"orderKey\":392,\"familyKey\":2373,\"genusKey\":2651126,\"subgenusKey\":null,\"speciesKey\":5275102,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288233\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Dryopteris dilatata auct. non (Hoffm.) A. Gray\",\"nubKey\":5275103,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-2.80792,\"latitude\":52.3699,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":1999,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":0,\"otherIssue\":0,\"unitQualifier\":null,\"modified\":1368692633000,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":\"GB\",\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307428506,\"kingdom\":\"Plantae\",\"phylum\":\"Magnoliophyta\",\"clazz\":\"Magnoliopsida\",\"order\":\"Fabales\",\"family\":\"Fabaceae\",\"genus\":\"Ulex\",\"subgenus\":null,\"species\":\"Ulex europaeus\",\"kingdomKey\":6,\"phylumKey\":49,\"classKey\":220,\"orderKey\":1370,\"familyKey\":5386,\"genusKey\":2951953,\"subgenusKey\":null,\"speciesKey\":2951984,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288443\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Ulex europaeus L.\",\"nubKey\":2951984,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-2.65837,\"latitude\":52.191,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":1999,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":0,\"otherIssue\":0,\"unitQualifier\":null,\"modified\":1368692634000,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":\"GB\",\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307428485,\"kingdom\":\"Plantae\",\"phylum\":\"Magnoliophyta\",\"clazz\":\"Magnoliopsida\",\"order\":\"Dipsacales\",\"family\":\"Valerianaceae\",\"genus\":\"Valeriana\",\"subgenus\":null,\"species\":\"Valeriana officinalis\",\"kingdomKey\":6,\"phylumKey\":49,\"classKey\":220,\"orderKey\":946,\"familyKey\":2508,\"genusKey\":2888741,\"subgenusKey\":null,\"speciesKey\":2888763,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288422\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Valeriana officinalis L.\",\"nubKey\":2888763,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-2.79982,\"latitude\":51.92037,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":1999,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":0,\"otherIssue\":0,\"unitQualifier\":null,\"modified\":1368692634000,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":\"GB\",\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307427915,\"kingdom\":\"Plantae\",\"phylum\":\"Magnoliophyta\",\"clazz\":\"Magnoliopsida\",\"order\":\"Asterales\",\"family\":\"Asteraceae\",\"genus\":\"Sonchus\",\"subgenus\":null,\"species\":\"Sonchus asper\",\"kingdomKey\":6,\"phylumKey\":49,\"classKey\":220,\"orderKey\":414,\"familyKey\":3065,\"genusKey\":3105646,\"subgenusKey\":null,\"speciesKey\":3105782,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60287852\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Sonchus oleraceus Wall.\",\"nubKey\":3105754,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-2.80628,\"latitude\":52.28,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":1999,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":0,\"otherIssue\":0,\"unitQualifier\":null,\"modified\":1368692633000,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":\"GB\",\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class)
    };
    
    Occurrence[] newOccurrences = new Occurrence[] {
      MAPPER.readValue("{\"key\":307428507,\"kingdom\":null,\"phylum\":null,\"clazz\":null,\"order\":null,\"family\":null,\"genus\":null,\"subgenus\":null,\"species\":null,\"kingdomKey\":null,\"phylumKey\":null,\"classKey\":null,\"orderKey\":null,\"familyKey\":null,\"genusKey\":null,\"subgenusKey\":null,\"speciesKey\":null,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288444\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":null,\"scientificName\":null,\"nubKey\":null,\"basisOfRecord\":\"OBSERVATION\",\"longitude\":null,\"latitude\":null,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":null,\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":null,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":null,\"otherIssue\":null,\"unitQualifier\":null,\"modified\":1378377506740,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":null,\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307428296,\"kingdom\":null,\"phylum\":null,\"clazz\":null,\"order\":null,\"family\":null,\"genus\":null,\"subgenus\":null,\"species\":null,\"kingdomKey\":null,\"phylumKey\":null,\"classKey\":null,\"orderKey\":null,\"familyKey\":null,\"genusKey\":null,\"subgenusKey\":null,\"speciesKey\":null,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288233\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":null,\"scientificName\":null,\"nubKey\":null,\"basisOfRecord\":\"OBSERVATION\",\"longitude\":null,\"latitude\":null,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":null,\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":null,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":null,\"otherIssue\":null,\"unitQualifier\":null,\"modified\":1378377507010,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":null,\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307428506,\"kingdom\":null,\"phylum\":null,\"clazz\":null,\"order\":null,\"family\":null,\"genus\":null,\"subgenus\":null,\"species\":null,\"kingdomKey\":null,\"phylumKey\":null,\"classKey\":null,\"orderKey\":null,\"familyKey\":null,\"genusKey\":null,\"subgenusKey\":null,\"speciesKey\":null,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288443\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":null,\"scientificName\":null,\"nubKey\":null,\"basisOfRecord\":\"OBSERVATION\",\"longitude\":null,\"latitude\":null,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":null,\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":null,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":null,\"otherIssue\":null,\"unitQualifier\":null,\"modified\":1378377506939,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":null,\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307428485,\"kingdom\":null,\"phylum\":null,\"clazz\":null,\"order\":null,\"family\":null,\"genus\":null,\"subgenus\":null,\"species\":null,\"kingdomKey\":null,\"phylumKey\":null,\"classKey\":null,\"orderKey\":null,\"familyKey\":null,\"genusKey\":null,\"subgenusKey\":null,\"speciesKey\":null,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60288422\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":null,\"scientificName\":null,\"nubKey\":null,\"basisOfRecord\":\"OBSERVATION\",\"longitude\":null,\"latitude\":null,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":null,\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":null,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":null,\"otherIssue\":null,\"unitQualifier\":null,\"modified\":1378377507103,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":null,\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class),
      MAPPER.readValue("{\"key\":307427915,\"kingdom\":null,\"phylum\":null,\"clazz\":null,\"order\":null,\"family\":null,\"genus\":null,\"subgenus\":null,\"species\":null,\"kingdomKey\":null,\"phylumKey\":null,\"classKey\":null,\"orderKey\":null,\"familyKey\":null,\"genusKey\":null,\"subgenusKey\":null,\"speciesKey\":null,\"occurrenceId\":null,\"institutionCode\":\"Botanical Society of the British Isles\",\"collectionCode\":\"6340\",\"catalogNumber\":\"60287852\",\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"owningOrgKey\":null,\"scientificName\":null,\"nubKey\":null,\"basisOfRecord\":\"OBSERVATION\",\"longitude\":null,\"latitude\":null,\"coordinateAccurracyInMeters\":null,\"coordinateAccurracy\":null,\"locality\":\"Site name protected\",\"county\":null,\"stateProvince\":null,\"country\":null,\"continent\":\"Europe\",\"collectorName\":null,\"identifierName\":null,\"identificationDate\":null,\"identificationRemarks\":null,\"identificationReferences\":null,\"occurrenceYear\":null,\"occurrenceMonth\":null,\"occurrenceDay\":null,\"occurrenceDate\":null,\"altitude\":null,\"depth\":null,\"remarks\":null,\"individualCount\":null,\"sex\":null,\"lifeStage\":null,\"establishmentMeans\":null,\"reproductiveCondition\":null,\"habitat\":null,\"behavior\":null,\"preparations\":null,\"disposition\":null,\"rights\":null,\"citation\":null,\"taxonomicIssue\":0,\"geospatialIssue\":null,\"otherIssue\":null,\"unitQualifier\":null,\"modified\":1378377507071,\"protocol\":\"DWC_ARCHIVE\",\"hostCountry\":null,\"identifiers\":[],\"images\":[],\"typeDesignations\":[],\"dataProviderId\":null,\"dataResourceId\":null,\"resourceAccessPointId\":null}", Occurrence.class)
    };
    
    Batch<LongOp> batch = new Batch<LongOp>();
    for (int i=0; i<oldOccurrences.length; i++){
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