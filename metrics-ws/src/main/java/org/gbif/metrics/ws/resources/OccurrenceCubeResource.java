/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.metrics.ws.resources;

import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.metrics.MetricsService;
import org.gbif.metrics.es.AggregationQuery;
import org.gbif.metrics.es.CountQuery;
import org.gbif.metrics.es.Parameter;
import org.gbif.metrics.ws.provider.ProvidedCountQuery;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Range;

/**
 * A simple generic resource that will look up a numerical count from the named cube and address
 * provided. Should no address be provided, a default builder which counts all records is used.
 */
@RestController
@RequestMapping(
    value = "occurrence",
    produces = {org.springframework.http.MediaType.APPLICATION_JSON_VALUE})
public class OccurrenceCubeResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceCubeResource.class);

  private final MetricsService metricsService;

  public OccurrenceCubeResource(MetricsService metricsService) {
    this.metricsService = metricsService;
  }

  /** Looks up an addressable count from the cube. */
  @GetMapping("count")
  public Long count(@ProvidedCountQuery CountQuery countQuery) {
    return metricsService.count(countQuery);
  }

  @GetMapping("counts/basisOfRecord")
  public Map<String, Long> getBasisOfRecordCounts() {
    return metricsService.countAggregation(AggregationQuery.ofBasisOfRecord());
  }

  @GetMapping("counts/countries")
  public Map<String, Long> getCountries(
      @RequestParam(value = "publishingCountry", required = false) String publishingCountry) {
    return metricsService.countAggregation(
        AggregationQuery.ofCountriesOfPublishingCountry(publishingCountry));
  }

  @GetMapping("counts/datasets")
  public Map<String, Long> getDatasets(
      @RequestParam(value = "country", required = false) String country,
      @RequestParam(value = "nubKey", required = false) Integer nubKey,
      @RequestParam(value = "taxonKey", required = false) Integer taxonKey) {
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

  @GetMapping("counts/kingdom")
  public Map<String, Long> getKingdomCounts() {
    return metricsService.countAggregation(AggregationQuery.ofKingdom());
  }

  @GetMapping("counts/publishingCountries")
  public Map<String, Long> getPublishingCountries(
      @RequestParam(value = "country", required = false) String country) {
    return metricsService.countAggregation(
        AggregationQuery.ofPublishingCountriesOfCountry(country));
  }

  @GetMapping("counts/year")
  public Map<String, Long> getYearCounts(
      @RequestParam(value = "year", required = false) String year) {
    Range<Integer> range = parseYearRange(year);
    return metricsService.countAggregation(
        AggregationQuery.ofYearRange(range.lowerEndpoint(), range.upperEndpoint()));
  }

  /** @return The public API schema */
  @GetMapping("count/schema")
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
        throw new IllegalArgumentException(
            "Valid year range between 1000 and now expected, separated by a comma");
      }
      return result;

    } catch (IllegalArgumentException ex) {
      LOG.error("Illegal year value {}", year, ex);
      throw new IllegalArgumentException("Parameter " + year + " is not a valid year range");
    }
  }
}
