package org.gbif.metrics.cube.occurrence;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.metrics.cube.bucketer.BasisOfRecordBucketer;
import org.gbif.metrics.cube.bucketer.CountryBucketer;
import org.gbif.metrics.cube.bucketer.EndpointTypeBucketer;
import org.gbif.metrics.cube.bucketer.OccurrenceIssueBucketer;
import org.gbif.metrics.cube.bucketer.TypeStatusBucketer;
import org.gbif.metrics.cube.bucketer.UUIDBucketer;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.DimensionAndBucketType;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.bucketers.BigEndianIntBucketer;
import com.urbanairship.datacube.bucketers.BooleanBucketer;
import com.urbanairship.datacube.ops.LongOp;

/**
 * The INTERNAL data cube definition for occurrence records.
 * This is the low level representation and changes should be carefully considered in advance considering the following:
 * <ol>
 * <li>All public API Dimensions should be declared here and with the correct type</li>
 * <li>The API mapping must be complete</li>
 * </ol>
 *
 * Note: It was by design to expose different objects in the public API, as the datacube objects are too complex, and
 * datacube brings in too many dependencies.  This also included the acceptance of collision in class names, but this is
 * constrained to a 2 places (here, and the HTTP parameter mapping in the web services) which was deemed acceptable for a nice
 * public API.
 */
public class OccurrenceCube {
  // number of bytes needed to store the types
  private final static int INT_BYTES = 4;
  private final static int BOOLEAN_BYTES = 1;

  // NOTE: Ensure that these are in sync with the public API, including the correct types
  // No ID substitution in place, and all dimensions are optional
  public static final Dimension<Country> COUNTRY = new Dimension<Country>("country", new CountryBucketer(), false, CountryBucketer.BYTES, true);
  public static final Dimension<Integer> YEAR = new Dimension<Integer>("year", new BigEndianIntBucketer(), false, INT_BYTES, true);
  // georeferenced is different from hasCoordinate and includes a no spatial issue check!
  public static final Dimension<Boolean> IS_GEOREFERENCED = new Dimension<Boolean>("georeferenced", new BooleanBucketer(), false, BOOLEAN_BYTES, true);
  public static final Dimension<BasisOfRecord> BASIS_OF_RECORD = new Dimension<BasisOfRecord>("basisOfRecord", new BasisOfRecordBucketer(), false, BasisOfRecordBucketer.BYTES, true);
  public static final Dimension<Country> PUBLISHING_COUNTRY = new Dimension<Country>("publishingCountry", new CountryBucketer(), false, CountryBucketer.BYTES, true);
  public static final Dimension<UUID> DATASET_KEY = new Dimension<UUID>("datasetKey", new UUIDBucketer(), false, UUIDBucketer.BYTES, true);
  public static final Dimension<Integer> TAXON_KEY = new Dimension<Integer>("taxonKey", new BigEndianIntBucketer(), false, INT_BYTES, true);
  public static final Dimension<EndpointType> PROTOCOL = new Dimension<EndpointType>("protocol", new EndpointTypeBucketer(), false, EndpointTypeBucketer.BYTES,true);
  public static final Dimension<TypeStatus> TYPE_STATUS = new Dimension<TypeStatus>("typeStatus", new TypeStatusBucketer(), false, TypeStatusBucketer.BYTES,true);
  public static final Dimension<OccurrenceIssue> ISSUE = new Dimension<OccurrenceIssue>("issue", new OccurrenceIssueBucketer(), false, OccurrenceIssueBucketer.BYTES,true);

  // Index mapping the public API cube dimensions to the internal datacube dimensions
  // NOTE: Ensure ALL public API mappings are covered
  public static final Map<org.gbif.api.model.metrics.cube.Dimension<?>, Dimension<?>> API_MAPPING =
    ImmutableMap.<org.gbif.api.model.metrics.cube.Dimension<?>, Dimension<?>>builder()
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.GEOREFERENCED, IS_GEOREFERENCED)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.BASIS_OF_RECORD, BASIS_OF_RECORD)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.COUNTRY, COUNTRY)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.PUBLISHING_COUNTRY, PUBLISHING_COUNTRY)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.DATASET_KEY, DATASET_KEY)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.TAXON_KEY, TAXON_KEY)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.PROTOCOL, PROTOCOL)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.YEAR, YEAR)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.TYPE_STATUS, TYPE_STATUS)
      .put(org.gbif.api.model.metrics.cube.OccurrenceCube.ISSUE, ISSUE)
    .build();

  // Singleton instance
  public static final DataCube<LongOp> INSTANCE = newInstance();

  // Not for instantiation
  private OccurrenceCube() {
  }

  /**
   * This uses the CubeIo as defined in the public API to construct an internal DataCube definition.
   * @return A new instance of the cube
   */
  private static DataCube<LongOp> newInstance() {
    // Dimensions as defined in the public API
    List<Dimension<?>> dimensions =
      ImmutableList.<Dimension<?>>copyOf(API_MAPPING.values()); // defensive copy

    // Rollups generated as defined in the public API
    // This could be done manually, but by using the API definition, we safeguard against developer oversight
    Builder<Rollup> b = ImmutableList.<Rollup>builder();
    for (org.gbif.api.model.metrics.cube.Rollup r : org.gbif.api.model.metrics.cube.OccurrenceCube.ROLLUPS) {
      com.google.common.collect.ImmutableSet.Builder<DimensionAndBucketType> sb = ImmutableSet.<DimensionAndBucketType>builder();
      for (org.gbif.api.model.metrics.cube.Dimension<?> d : r.getDimensions()) {
        sb.add(new DimensionAndBucketType(API_MAPPING.get(d)));
      }
      b.add(new Rollup(sb.build()));
    }
    List<Rollup> rollups = b.build();

    return new DataCube<LongOp>(dimensions, rollups);
  }
}
