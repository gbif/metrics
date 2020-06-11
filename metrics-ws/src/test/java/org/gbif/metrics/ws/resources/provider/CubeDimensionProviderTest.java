package org.gbif.metrics.ws.resources.provider;

import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.metrics.es.CountQuery;

import java.util.UUID;

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.gbif.metrics.ws.provider.CountQueryArgumentResolver;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests that the Jersey provider does it's job unpacking the HTTP params and constructing the internal datacube
 * read builder properly.
 */
public class CubeDimensionProviderTest {

  private final UUID DS = UUID.randomUUID();

  // Builds an Address (package declarations necessary, as this is the INTERNAL one)
  private CountQuery getInternallyBuilt() {
    return new CountQuery()
      .withParameter(OccurrenceCube.BASIS_OF_RECORD.getKey(), BasisOfRecord.OBSERVATION.name())
      .withParameter(OccurrenceCube.TAXON_KEY.getKey(), "212")
      .withParameter(OccurrenceCube.DATASET_KEY.getKey(), DS.toString());
  }

  // An address built by the Jersey provider must be the same as a DC readbuilder version
  @Test
  public void testBuild() {
    CountQueryArgumentResolver b = new CountQueryArgumentResolver(null);
    MultivaluedMap<String, String> m = new MultivaluedMapImpl();
    m.add(OccurrenceCube.BASIS_OF_RECORD.getKey(), BasisOfRecord.OBSERVATION.name());
    m.add(OccurrenceCube.TAXON_KEY.getKey(), "212");
    m.add(OccurrenceCube.DATASET_KEY.getKey(), DS.toString());
    CountQuery countQuery = b.build(m);
    assertEquals(getInternallyBuilt(), countQuery);
  }

  @Ignore
  @Test(expected = IllegalArgumentException.class)
  public void testFailureScenarios() {
    CountQueryArgumentResolver b = new CountQueryArgumentResolver(null);
    MultivaluedMap<String, String> m = new MultivaluedMapImpl();
    m.add("nonesense", "should throw error");
    b.build(m);
  }

}
