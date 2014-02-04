package org.gbif.metrics.ws.resources;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.metrics.cube.CubeIo;
import org.gbif.metrics.cube.CubeIo.Type;
import org.gbif.metrics.cube.index.common.UuidIntMap;
import org.gbif.metrics.cube.index.country.OccurrenceDatasetCountryCube;
import org.gbif.metrics.cube.index.taxon.OccurrenceDatasetCube;
import org.gbif.metrics.cube.occurrence.OccurrenceCube;
import org.gbif.metrics.ws.resources.provider.ProvidedOccurrenceCubeReader;
import org.gbif.ws.util.ExtraMediaTypes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.validation.constraints.Min;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.DataCubeIo;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.ReadBuilder;
import com.urbanairship.datacube.ops.LongOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple generic resource that will look up a numerical count from the named cube
 * and address provided. Should no address be provided, a default builder which counts
 * all records is used.
 */
@Path("/occurrence")
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
public class OccurrenceCubeResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceCubeResource.class);

  private static Ordering<Map.Entry<?, Integer>> MAP_ORDER_DESC_INT = Ordering.natural()
    .onResultOf(new Function<Map.Entry<?, Integer>, Integer>() {

      public Integer apply(Map.Entry<?, Integer> entry) {
        return entry.getValue();
      }
    }).reverse();

  private static Ordering<Map.Entry<?, Long>> MAP_ORDER_DESC_LONG = Ordering.natural()
    .onResultOf(new Function<Map.Entry<?, Long>, Long>() {

      public Long apply(Map.Entry<?, Long> entry) {
        return entry.getValue();
      }
    }).reverse();

  // Since the cube does not support a count for all records, we do a basic sum
  // Total = georeferenced + nonGeoferenced records
  private static final Address GEOREFERENCED = new ReadBuilder(OccurrenceCube.INSTANCE)
    .at(OccurrenceCube.IS_GEOREFERENCED, true).build();

  private static final Address NOT_GEOREFERENCED = new ReadBuilder(OccurrenceCube.INSTANCE)
    .at(OccurrenceCube.IS_GEOREFERENCED, false).build();

  private final DataCubeIo<LongOp> occCubeIo;
  private final DataCubeIo<UuidIntMap> taxCubeIo;
  private final DataCubeIo<UuidIntMap> countryCubeIo;


  @Inject
  public OccurrenceCubeResource(@CubeIo(Type.OCCURRENCE) DataCubeIo<LongOp> occCubeIo,
    @CubeIo(Type.TAXON_OCCURRENCE_DATASET) DataCubeIo<UuidIntMap> taxCubeIo,
    @CubeIo(Type.COUNTRY_OCCURRRENCE_DATASET) DataCubeIo<UuidIntMap> countryCubeIo) {
    this.occCubeIo = occCubeIo;
    this.taxCubeIo = taxCubeIo;
    this.countryCubeIo = countryCubeIo;
  }

  /**
   * Looks up an addressable count from the cube, using the given {@link ReadBuilder}.
   */
  @GET
  @Path("/count")
  public Long count(@ProvidedOccurrenceCubeReader ReadBuilder b) {
    Address a = b.build();
    try {
      // If no address, perform a SUM to get all
      if (a.getBuckets().isEmpty()) {
        return lookup(GEOREFERENCED) + lookup(NOT_GEOREFERENCED);
      } else {
        return lookup(a);
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("The provided address is not calculated in the cube: " + a);
    } catch (Exception e) {
      LOG.error("Unable to read from the cube", e);
      throw new ServiceUnavailableException("Unable to read from the occurrence cube", e);
    }
  }

  @GET
  @Path("/counts/basis_of_record")
  public Map<BasisOfRecord, Long> getBasisOfRecordCounts() {
    Map<BasisOfRecord, Long> distribution = Maps.newHashMap();
    try {
      for (BasisOfRecord basisOfRecord : BasisOfRecord.values()) {
        final Address address =
          new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.BASIS_OF_RECORD, basisOfRecord).build();
        distribution.put(basisOfRecord, lookup(address));
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Error getting the occurrence counts", e);
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error getting the occurrence counts", e);
    }
    return sortDescendingValues(distribution);
  }

  @GET
  @Path("/counts/countries")
  public Map<Country, Long> getCountries(@QueryParam("publishingCountry") String publishingCountry) {
    return getCountryMap(Country.fromIsoCode(publishingCountry), true);
  }


  @GET
  @Path("/counts/datasets")
  public Map<UUID, Integer> getDatasets(@QueryParam("country") String country,
    @QueryParam("nubKey") Integer nubKey) {
    if (country != null && nubKey != null) {
      throw new IllegalArgumentException("Only one filter parameter [country/nubKey] is allowed");
    }
    if (nubKey != null) {
      return getDatasetsByNub(nubKey);
    } else {
      return getDatasetsByCountry(Country.fromIsoCode(country));
    }
  }

  @GET
  @Path("/counts/kingdom")
  public Map<Kingdom, Long> getKingdomCounts() {
    Map<Kingdom, Long> distribution = Maps.newHashMap();
    try {
      for (Kingdom kingdom : Kingdom.values()) {
        final Address address =
          new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.NUB_KEY, kingdom.nubUsageID()).build();
        distribution.put(kingdom, lookup(address));
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Error getting the occurrence counts", e);
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error getting the occurrence counts", e);
    }
    return sortDescendingValues(distribution);
  }

  @GET
  @Path("/counts/publishing_countries")
  public Map<Country, Long> getPublishingCountries(@QueryParam("country") String country) {
    return getCountryMap(Country.fromIsoCode(country), false);
  }

  /**
   * @return The public API schema
   */
  @GET
  @Path("/count/schema")
  public List<Rollup> getSchema() {
    // External Occurrence cube definition
    return org.gbif.api.model.metrics.cube.OccurrenceCube.ROLLUPS;
  }


  @GET
  @Path("/counts/year")
  public Map<Integer, Long> getYearCounts(@QueryParam("from") @Min(0) int from, @QueryParam("to") @Min(0) int to) {
    Preconditions.checkArgument(from <= to, "Parameters are not a valid range of years");
    ImmutableSortedMap.Builder<Integer, Long> distribution = ImmutableSortedMap.naturalOrder();
    try {
      for (int year = from; year < to; year++) {
        final Address address =
          new ReadBuilder(OccurrenceCube.INSTANCE).at(OccurrenceCube.YEAR, year).build();
        distribution.put(year, lookup(address));
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Error getting the occurrence counts", e);
    } catch (Exception e) {
      throw new ServiceUnavailableException("Error getting the occurrence counts", e);
    }
    return distribution.build();
  }

  /**
   * @return an immutable copy of the original, with the types casted and ordered
   *         by the value descending
   */
  @VisibleForTesting
  Map<UUID, Integer> sortDescending(Map<CharSequence, Integer> source) {
    // Convert CharSequence to UUID map
    Map<UUID, Integer> map = Maps.newHashMap();
    for (Map.Entry<CharSequence, Integer> e : source.entrySet()) {
      map.put(UUID.fromString(String.valueOf(e.getKey())), e.getValue());
    }

    return sort(map, MAP_ORDER_DESC_INT);
  }

  @VisibleForTesting
  <K extends Comparable> Map<K, Long> sortDescendingValues(Map<K, Long> source) {
    return sort(source, MAP_ORDER_DESC_LONG);
  }

  private <K extends Comparable<K>, V> Map<K, V> sort(Map<K, V> source, Ordering<Map.Entry<?, V>> ordering) {
    ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
    // we need a compund ordering to guarantee stable order with identical values

    Ordering<Map.Entry<K, V>> keyOrder = Ordering.natural().onResultOf(new Function<Map.Entry<K, V>, K>() {
        public K apply(Map.Entry<K, V> entry) {
          return entry.getKey();
        }
      });

    for (Map.Entry<K, V> entry : ordering.compound(keyOrder).sortedCopy(source.entrySet())) {
      builder.put(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  /**
   * @param isPublishingCountry if given country parameter is a publishing country
   */
  private Map<Country, Long> getCountryMap(Country country, boolean isPublishingCountry) {
    ReadBuilder b = new ReadBuilder(OccurrenceCube.INSTANCE);
    if (isPublishingCountry) {
      b.at(OccurrenceCube.PUBLISHING_COUNTRY, country);
    } else {
      b.at(OccurrenceCube.COUNTRY, country);
    }

    Map<Country, Long> map = Maps.newHashMap();
    // lacking a specific cube we iterate over all official countries to generate a sorted map
    try {
      // we reuse the same address instance with changing dimension value below
      // the read builder is attached to the same instance all the time and only the latest value for a given dimension
      // is kept
      Address address = b.build();
      for (Country c : Country.values()) {
        if (c.isOfficial()) {
          if (isPublishingCountry) {
            b.at(OccurrenceCube.COUNTRY, c);
          } else {
            b.at(OccurrenceCube.PUBLISHING_COUNTRY, c);
          }

          long cnt = lookup(address);
          if (cnt > 0) {
            map.put(c, cnt);
          }
        }
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Unable to read from the occurrence cube", e);
    } catch (InterruptedException e) {
      throw new ServiceUnavailableException("Unable to read from the occurrence cube", e);
    }

    return sortDescendingValues(map);
  }

  private Map<UUID, Integer> getDatasetsByCountry(Country country) {
    return getMapFromCube(countryCubeIo, OccurrenceDatasetCountryCube.INSTANCE, OccurrenceDatasetCountryCube.COUNTRY,
      country);
  }

  private Map<UUID, Integer> getDatasetsByNub(int nubKey) {
    return getMapFromCube(taxCubeIo, OccurrenceDatasetCube.INSTANCE, OccurrenceDatasetCube.NUB_KEY, nubKey);
  }

  /**
   * Gets the a sorted Map<UUID, Integer> UuidIntMap from a DataCube<UuidIntMap>.
   */
  private <O> Map<UUID, Integer> getMapFromCube(DataCubeIo<UuidIntMap> dataCubeIo, DataCube<UuidIntMap> dataCube,
    Dimension<?> dimension, O coordinate) {
    try {
      Optional<UuidIntMap> o = null;
      if (coordinate != null) {
        o = dataCubeIo.get(new ReadBuilder(dataCube).at(dimension, coordinate));
      }
      if (o != null && o.isPresent()) {
        return sortDescending(o.get().getCounts());
      } else {
        return sortDescending(UuidIntMap.EMPTY_MAP.getCounts());
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Unable to read from the cube", e);
      throw new ServiceUnavailableException("Unable to read from the taxon occurrence dataset cube", e);
    }
  }

  /**
   * Simple lookup utility
   *
   * @param a To lookup
   * @return The count, which may be calculated to be 0
   * @throws IOException On communication to the cube (typically an HBase layer)
   * @throws InterruptedException If something nasty happens within the cube (e.g.) some fatal service error
   * @throws IllegalArgumentException Should the provided Address NOT be calculated in the cube
   */
  private long lookup(Address a) throws IOException, InterruptedException, IllegalArgumentException {
    Optional<LongOp> o = occCubeIo.get(a);
    if (o.isPresent()) {
      return o.get().getLong();
    } else {
      return 0L;
    }
  }
}
