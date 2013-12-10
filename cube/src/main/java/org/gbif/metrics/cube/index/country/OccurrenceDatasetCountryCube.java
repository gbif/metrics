package org.gbif.metrics.cube.index.country;

import org.gbif.api.vocabulary.Country;
import org.gbif.metrics.cube.bucketer.CountryBucketer;
import org.gbif.metrics.cube.index.common.UuidIntMap;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.Rollup;

/**
 * The cube definition for holding an index of datasets having occurrences.
 * This cube allows you to retreive in a single call, the complete listing of datasets
 * and the counts within the dataset.  
 * Because DataCube does not support any scanning
 * operations, it is not possible to treat dataset and host country as separate dimensions, 
 * as one could not ask the cube "what datasets are there for country X".  One can simply
 * ask for the complete representation for the host country.  This means mutations are expensive
 * but in this implementation, necessary.   
 */
public class OccurrenceDatasetCountryCube {
  public static final Dimension<Country> COUNTRY = new Dimension<Country>("country", new CountryBucketer(), false, CountryBucketer.BYTES, true);

  // Singleton instance 
  public static final DataCube<UuidIntMap> INSTANCE = newInstance();

  // Not for instantiation
  private OccurrenceDatasetCountryCube() {
  }
  
  private static DataCube<UuidIntMap> newInstance() {
    List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(COUNTRY); 
    List<Rollup> rollups = ImmutableList.of(new Rollup(COUNTRY));
    return new DataCube<UuidIntMap>(dimensions, rollups);
  }
}
