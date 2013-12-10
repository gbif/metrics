package org.gbif.metrics.cube.occurrence;

import org.gbif.api.model.metrics.cube.Dimension;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.AsyncException;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.DataCubeIo;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.ReadBuilder;
import com.urbanairship.datacube.SyncLevel;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * This is a test of the general functionality for the occurrence cube, the rollups
 * and inherently includes verification that the normalization provided by {@link OccurrenceAddressUtil} operates as
 * expected.
 */
public class OccurrenceCubeTest {

  private DataCubeIo<LongOp> cubeIo;

  // Sets up the cube
  @Before
  public void setup() {
    ConcurrentMap<BoxedByteArray, byte[]> backingMap = Maps.newConcurrentMap();
    IdService idService = new CachingIdService(4, new MapIdService(), "id");
    DbHarness<LongOp> dbHarness =
      new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, CommitType.READ_COMBINE_CAS, idService);
    cubeIo = new DataCubeIo<LongOp>(OccurrenceCube.INSTANCE, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);
  }


  @Test
  public void testCompleteness() throws InterruptedException, AsyncException, IOException {
    for (Dimension<?> dim : org.gbif.api.model.metrics.cube.OccurrenceCube.DIMENSIONS) {
      assertTrue("OccurrenceCube.API_MAPPING missing API dimension " + dim.getKey(),
        OccurrenceCube.API_MAPPING.containsKey(dim));
    }
  }

  @Test
  public void testRollups() throws InterruptedException, AsyncException, IOException {
    final UUID ds1 = UUID.randomUUID();
    final UUID ds2 = UUID.randomUUID();
    final Occurrence o1 =
      occurrenceOf(123456, 1, 2, 3, 4, 5, 6, 7, 7, ds1, BasisOfRecord.OBSERVATION, Country.UKRAINE, 2012, 10.0, 11.0,
        EndpointType.BIOCASE);
    final Occurrence o2 =
      occurrenceOf(123457, 1, 2, 3, 4, 5, 6, 7, 7, ds1, BasisOfRecord.PRESERVED_SPECIMEN, Country.UKRAINE, 2012, 10.0,
        11.0, EndpointType.DWC_ARCHIVE);
    final Occurrence o3 =
      occurrenceOf(123458, 1, 2, 3, 4, 5, 6, 8, 9, ds2, BasisOfRecord.PRESERVED_SPECIMEN, Country.UKRAINE, 2012, 10.0,
        11.0, EndpointType.DIGIR);

    cubeIo.writeAsync(OccurrenceAddressUtil.cubeMutation(o1, new LongOp(1)));
    cubeIo.writeAsync(OccurrenceAddressUtil.cubeMutation(o2, new LongOp(1)));
    cubeIo.writeAsync(OccurrenceAddressUtil.cubeMutation(o3, new LongOp(1)));
    cubeIo.flush();

    // Tests that NUB KEY are correctly normalized
    Assert.assertEquals(3L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, 1)));
    Assert.assertEquals(2L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, 7)));
    Assert.assertEquals(1L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, 8)));
    Assert.assertEquals(1L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, 9)));

    // Assertion that normalization of taxa don't incorrectly screw counts (http://dev.gbif.org/issues/browse/MET-11)
    Assert.assertEquals(1L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.DATASET_KEY, ds2)));

    // Random checking
    Assert.assertEquals(1L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.DATASET_KEY, ds2)));
    Assert.assertEquals(
      0L,
      getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.BASIS_OF_RECORD,
        BasisOfRecord.FOSSIL_SPECIMEN)
        .at(OccurrenceCube.HOST_COUNTRY, Country.AFGHANISTAN)));
  }

  @Test
  public void testSubtraction() throws InterruptedException, AsyncException, IOException {
    final UUID ds1 = UUID.randomUUID();
    final Occurrence o1 =
      occurrenceOf(123456, 1, 2, 3, 4, 5, 6, 7, 7, ds1, BasisOfRecord.OBSERVATION, Country.UKRAINE, 2012, 10.0, 11.0,
        EndpointType.DWC_ARCHIVE);

    cubeIo.writeAsync(OccurrenceAddressUtil.cubeMutation(o1, new LongOp(1)));
    cubeIo.writeAsync(OccurrenceAddressUtil.cubeMutation(o1, new LongOp(1)));
    cubeIo.flush();
    Assert.assertEquals(2L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, 1)));
    Assert.assertEquals(2L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.DATASET_KEY, ds1)));

    cubeIo.writeAsync(OccurrenceAddressUtil.cubeMutation(o1, new LongOp(-1)));
    cubeIo.flush();
    Assert.assertEquals(1L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, 1)));
    Assert.assertEquals(1L, getCount(new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.DATASET_KEY, ds1)));
  }

  private long getCount(ReadBuilder r) throws IOException, InterruptedException {
    Optional<LongOp> opt = cubeIo.get(r);
    if (!opt.isPresent()) {
      return 0L;
    } else {
      return opt.get().getLong();
    }
  }

  private Occurrence occurrenceOf(
    Integer key, Integer kingdom, Integer phylum, Integer classs, Integer order, Integer family, Integer genus,
    Integer species, Integer nub,
    UUID dataset, BasisOfRecord bor, Country country, Integer year, Double latitude, Double longitude,
    EndpointType protocol) {
    return Occurrence.builder()
      .key(key)
      .kingdomKey(kingdom)
      .phylumKey(phylum)
      .classKey(classs)
      .orderKey(order)
      .familyKey(family)
      .genusKey(genus)
      .speciesKey(species)
      .nubKey(nub)
      .datasetKey(dataset)
      .basisOfRecord(bor)
      .country(country)
      .occurrenceYear(year)
      .latitude(latitude)
      .longitude(longitude)
      .protocol(protocol)
      .build();
  }
}
