package org.gbif.metrics.cube.index.taxon;

import org.gbif.metrics.cube.index.common.UuidIntMap;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.bucketers.BigEndianIntBucketer;

/**
 * The cube definition for holding an index of datasets having occurrences.
 * This cube allows you to retreive in a single call, the complete listing of datasets
 * and the counts within the dataset.  
 * Because DataCube does not support any scanning
 * operations, it is not possible to treat dataset and nub key as separate dimensions, 
 * as one could not ask the cube "what datasets are there for nubKey X".  One can simply
 * ask for the complete representation for the nub key.  This means mutations are expensive
 * but in this implementation, necessary.   
 */
public class OccurrenceDatasetCube {
  // number of bytes needed to store the types 
  private final static int INT_BYTES = 4;
  public static final Dimension<Integer> NUB_KEY = new Dimension<Integer>("nubKey", new BigEndianIntBucketer(), false, INT_BYTES, true);

  // Singleton instance 
  public static final DataCube<UuidIntMap> INSTANCE = newInstance();

  // Not for instantiation
  private OccurrenceDatasetCube() {
  }
  
  private static DataCube<UuidIntMap> newInstance() {
    List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(NUB_KEY); 
    List<Rollup> rollups = ImmutableList.of(new Rollup(NUB_KEY));
    return new DataCube<UuidIntMap>(dimensions, rollups);
  }
}
