package org.gbif.metrics.ws.resources.provider;

import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.vocabulary.BasisOfRecord;

import java.util.UUID;

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.ReadBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the Jersey provider does it's job unpacking the HTTP params and constructing the internal datacube
 * read builder properly.
 */
public class CubeDimensionProviderTest {

  private final UUID DS = UUID.randomUUID();

  // Builds an Address (package declarations necessary, as this is the INTERNAL one)
  private Address getInternallyBuilt() {
    return new ReadBuilder(org.gbif.metrics.cube.occurrence.OccurrenceCube.INSTANCE)
      .at(org.gbif.metrics.cube.occurrence.OccurrenceCube.BASIS_OF_RECORD, BasisOfRecord.OBSERVATION)
      .at(org.gbif.metrics.cube.occurrence.OccurrenceCube.TAXON_KEY, 212)
      .at(org.gbif.metrics.cube.occurrence.OccurrenceCube.DATASET_KEY, DS)
      .build();
  }

  // An address built by the Jersey provider must be the same as a DC readbuilder version
  @Test
  public void testBuild() {
    OccurrenceCubeReaderProvider b = new OccurrenceCubeReaderProvider(null);
    MultivaluedMap<String, String> m = new MultivaluedMapImpl();
    m.add(OccurrenceCube.BASIS_OF_RECORD.getKey(), BasisOfRecord.OBSERVATION.toString());
    m.add(OccurrenceCube.TAXON_KEY.getKey(), "212");
    m.add(OccurrenceCube.DATASET_KEY.getKey(), DS.toString());
    ReadBuilder rb = b.build(m);
    assertEquals(getInternallyBuilt(), rb.build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailureScenarios() {
    OccurrenceCubeReaderProvider b = new OccurrenceCubeReaderProvider(null);
    MultivaluedMap<String, String> m = new MultivaluedMapImpl();
    m.add("nonesense", "should throw error");
    b.build(m);
  }

}
