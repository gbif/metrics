/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.metrics.es;

import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Kingdom;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** Metrics query to obtain simple dimensions/fields and counts based on a list of parameters. */
public class AggregationQuery {

  // Transforms a string into a Country.name()
  private static final Function<String, String> COUNTRY_LABEL_TRANSFORM =
      key -> Country.fromIsoCode(key).name();

  // Pre-defined aggregations
  private static final AggregationQuery COUNTRY =
      new AggregationQuery("country", COUNTRY_LABEL_TRANSFORM);
  private static final AggregationQuery PUBLISHING_COUNTRY =
      new AggregationQuery("publishingCountry", COUNTRY_LABEL_TRANSFORM);
  private static final AggregationQuery BASIS_OF_RECORD =
      new AggregationQuery(
          "basisOfRecord", key -> VocabularyUtils.lookupEnum(key, BasisOfRecord.class).name());
  private static final AggregationQuery KINGDOM =
      new AggregationQuery("kingdom", key -> VocabularyUtils.lookupEnum(key, Kingdom.class).name());

  private final String dimension;

  private final Set<Parameter> parameters;

  private final Function<String, String> keyLabelTransform;

  private AggregationQuery(String dimension, Set<Parameter> parameters) {
    this.dimension = dimension;
    this.parameters = parameters;
    this.keyLabelTransform = Function.identity();
  }

  private AggregationQuery(
      String dimension, Set<Parameter> parameters, Function<String, String> keyLabelTransform) {
    this.dimension = dimension;
    this.parameters = parameters;
    this.keyLabelTransform = keyLabelTransform;
  }

  private AggregationQuery(String dimension, Function<String, String> keyLabelTransform) {
    this.dimension = dimension;
    parameters = new HashSet<>();
    this.keyLabelTransform = keyLabelTransform;
  }

  // Factory methods for supported aggregations

  public static AggregationQuery ofBasisOfRecord() {
    return BASIS_OF_RECORD;
  }

  public static AggregationQuery ofKingdom() {
    return KINGDOM;
  }

  public static AggregationQuery ofCountriesOfPublishingCountry(String publishingCountry) {
    return Optional.ofNullable(publishingCountry)
        .map(
            pc ->
                new AggregationQuery(
                    "country",
                    Collections.singleton(new Parameter("publishingCountry", publishingCountry)),
                    COUNTRY_LABEL_TRANSFORM))
        .orElse(COUNTRY);
  }

  public static AggregationQuery ofPublishingCountriesOfCountry(String country) {
    return Optional.ofNullable(country)
        .map(
            pc ->
                new AggregationQuery(
                    "publishingCountry",
                    Collections.singleton(new Parameter("country", country)),
                    COUNTRY_LABEL_TRANSFORM))
        .orElse(PUBLISHING_COUNTRY);
  }

  public static AggregationQuery ofYearRange(int lowerBound, int upperBound) {
    return new AggregationQuery(
        "year", Collections.singleton(new Parameter("year", lowerBound + "," + upperBound)));
  }

  public static AggregationQuery ofDatasets(Set<Parameter> parameters) {
    return new AggregationQuery("datasetKey", parameters);
  }

  /** @return dimension/field to be queried */
  public String getDimension() {
    return dimension;
  }

  /** @return list filter parameters */
  public Collection<Parameter> getParameters() {
    return parameters;
  }

  /** @return a function used to transform the dimension into the Elasticsearch field name */
  public Function<String, String> getKeyLabelTransform() {
    return keyLabelTransform;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AggregationQuery that = (AggregationQuery) o;
    // Ignore the keyLabelTransform
    return dimension.equals(that.dimension) && parameters.equals(that.parameters);
  }

  @Override
  public int hashCode() {
    // Ignore the keyLabelTransform
    return Objects.hash(dimension, parameters);
  }
}
