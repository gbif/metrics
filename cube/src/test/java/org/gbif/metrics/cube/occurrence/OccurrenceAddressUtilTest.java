package org.gbif.metrics.cube.occurrence;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.TypeStatus;

import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.BucketType;
import com.urbanairship.datacube.BucketTypeAndBucket;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.ops.LongOp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class OccurrenceAddressUtilTest {

  @Test
  public void testCubeMutation() throws Exception {
    final UUID datasetKey = UUID.randomUUID();
    Occurrence occ = new Occurrence();
    occ.setTypeStatus(TypeStatus.HOLOTYPE);
    occ.setDatasetKey(datasetKey);
    occ.setKey(1);
    occ.setAltitude(110);
    occ.setDepth(10);
    occ.setBasisOfRecord(BasisOfRecord.PRESERVED_SPECIMEN);
    occ.setTaxonKey(1000);
    occ.setKingdomKey(6);
    occ.setPhylumKey(66);
    occ.setClassKey(666);
    occ.setOrderKey(6666);
    occ.setFamilyKey(66666);
    occ.setGenusKey(666666);
    occ.setSubgenusKey(6666666);
    occ.setSpeciesKey(66666666);
    occ.setContinent(Continent.EUROPE);
    occ.setCountry(Country.ALBANIA);
    occ.setProtocol(EndpointType.DWC_ARCHIVE);
    occ.setPublishingCountry(Country.ITALY);
    occ.setYear(1992);
    occ.setMonth(1);
    occ.setDay(31);

    Batch<LongOp> updates = OccurrenceAddressUtil.cubeMutation(occ, new LongOp(1));

    // make sure we have a single dimension rollup for each dimension
    Set<Dimension<?>> addressed = Sets.newHashSet();
    int nubCounter = 0;
    for (Address a : updates.getMap().keySet()) {
      Dimension<?> singleDim = singleDimensionAddress(a);
      if (singleDim != null) {
        if (OccurrenceCube.NUB_KEY.equals(singleDim)) {
          nubCounter++;
        } else if (addressed.contains(singleDim)){
          System.out.println("Warning, single dimension "+singleDim+" used multiple times");
          System.out.println(a);
        }
        addressed.add(singleDim);
      }
    }

    System.out.println(OccurrenceCube.API_MAPPING.values());
    System.out.println(addressed);
    assertEquals("Not all higher taxa are mutated", 9, nubCounter);
    assertEquals("Not all cube dimensions are mutated", OccurrenceCube.API_MAPPING.size(), addressed.size());

  }

  private Dimension<?> singleDimensionAddress(Address a) {
    Set<Dimension<?>> nonWildcardDims = Sets.newHashSet();
    for (Dimension<?> dim : OccurrenceCube.API_MAPPING.values()) {
      assertNotNull(a.get(dim));
      BucketTypeAndBucket bucket = a.get(dim);
      if (!bucket.bucketType.equals(BucketType.WILDCARD)) {
        nonWildcardDims.add(dim);
      }
    }
    if (nonWildcardDims.size()==1) {
      return nonWildcardDims.iterator().next();
    }
    return null;
  }

}
