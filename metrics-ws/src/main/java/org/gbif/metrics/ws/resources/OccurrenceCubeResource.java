package org.gbif.metrics.ws.resources;

import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.metrics.MetricsService;
import org.gbif.metrics.es.AggregationQuery;
import org.gbif.metrics.es.CountQuery;
import org.gbif.metrics.es.Parameter;
import org.gbif.metrics.ws.resources.provider.ProvidedCountQuery;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.google.inject.Inject;
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

  private final MetricsService metricsService;


  @Inject
  public OccurrenceCubeResource(MetricsService metricsService) {
    this.metricsService = metricsService;
  }

  /**
   * Looks up an addressable count from the cube.
   */
  @GET
  @Path("/count")
  public Long count(@ProvidedCountQuery CountQuery countQuery) {
    return metricsService.count(countQuery);
  }

  @GET
  @Path("/counts/basisOfRecord")
  public Map<String, Long> getBasisOfRecordCounts() {
    return metricsService.countAggregation(AggregationQuery.ofBasisOfRecord());
  }

  @GET
  @Path("/counts/countries")
  public Map<String, Long> getCountries(@QueryParam("publishingCountry") String publishingCountry) {
    return metricsService.countAggregation(AggregationQuery.ofCountriesOfPublishingCountry(publishingCountry));
  }


  @GET
  @Path("/counts/datasets")
  public Map<String, Long> getDatasets(@QueryParam("country") String country,
    @QueryParam("nubKey") Integer nubKey, @QueryParam("taxonKey") Integer taxonKey) {
    Set<Parameter> parameters = new HashSet<>();
    if (country != null) {
      parameters.add(new Parameter("country", country));
    }

    if (taxonKey != null) {
      parameters.add(new Parameter("taxonKey", taxonKey.toString()));
    }

    if (nubKey != null && taxonKey == null) {
      parameters.add(new Parameter("taxonKey", nubKey.toString()));
    }
    return metricsService.countAggregation(AggregationQuery.ofDatasets(parameters));
  }

  @GET
  @Path("/counts/kingdom")
  public Map<String, Long> getKingdomCounts() {
    return metricsService.countAggregation(AggregationQuery.ofKingdom());
  }

  @GET
  @Path("/counts/publishingCountries")
  public Map<String, Long> getPublishingCountries(@QueryParam("country") String country) {
    return metricsService.countAggregation(AggregationQuery.ofPublishingCountriesOfCountry(country));
  }

  @GET
  @Path("/counts/year")
  public Map<String, Long> getYearCounts(@QueryParam("year") String year) {
    Range<Integer> range = parseYearRange(year);
    return metricsService.countAggregation(AggregationQuery.ofYearRange(range.lowerEndpoint(), range.upperEndpoint()));
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

  @VisibleForTesting
  protected static Range<Integer> parseYearRange(String year) {
    final int now = 1901 + new Date().getYear();
    if (Strings.isNullOrEmpty(year)) {
      // return all years between 1500 and now
      return Range.open(1500, now);
    }
    try {
      Range<Integer> result = null;
      String[] years = year.split(",");
      if (years.length == 1) {
        result = Range.open(Integer.parseInt(years[0].trim()), now);

      } else if (years.length == 2) {
        result = Range.open(Integer.parseInt(years[0].trim()), Integer.parseInt(years[1].trim()));

      }

      // verify upper and lower bounds are sensible
      if (result == null || result.lowerEndpoint() < 1000 || result.upperEndpoint() > now) {
        throw new IllegalArgumentException("Valid year range between 1000 and now expected, separated by a comma");
      }
      return result;

    } catch (IllegalArgumentException ex) {
      LOG.error("Illegal year value {}", year, ex);
      throw new IllegalArgumentException("Parameter "+ year +" is not a valid year range");
    }
  }


}
