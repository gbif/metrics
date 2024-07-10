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
package org.gbif.metrics.es;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;

/** Query parameter. */
@Getter
@AllArgsConstructor
public class Parameter {

  private final String name;
  private final Object value;
  private final ParameterType type;

  public Parameter(String name, String value) {
    this.name = name;
    this.type = determineType(name);
    this.value = parseValue(value, this.type);
  }

  private ParameterType determineType(String name) {
    switch (name) {
      case "basisOfRecord":
        return ParameterType.BASIS_OF_RECORD;
      case "country":
      case "publishingCountry":
        return ParameterType.COUNTRY;
      case "datasetKey":
        return ParameterType.UUID;
      case "isGeoreferenced":
        return ParameterType.BOOLEAN;
      case "issue":
        return ParameterType.OCCURRENCE_ISSUE;
      case "protocol":
        return ParameterType.ENDPOINT_TYPE;
      case "taxonKey":
        return ParameterType.INTEGER;
      case "typeStatus":
        return ParameterType.TYPE_STATUS;
      case "year":
        return ParameterType.RANGE;
      case "countQuery":
        return ParameterType.STRING;
      default:
        throw new IllegalArgumentException("Invalid parameter name: " + name);
    }
  }

  private Object  parseValue(String value, ParameterType type) {
    switch (type) {
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case INTEGER:
        try {
          return Integer.parseInt(value);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid integer value: " + value);
        }
      case STRING:
        return value;
      case UUID:
        try {
          return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid UUID value: " + value);
        }
      case BASIS_OF_RECORD:
        return VocabularyUtils.lookup(value, BasisOfRecord.class)
          .orElseThrow(() -> new IllegalArgumentException("Invalid basis of record: " + value));
      case COUNTRY:
        Optional<Country> countryOptional = VocabularyUtils.lookup(value, Country.class);
        if (countryOptional.isPresent()) {
          return countryOptional.get();
        }
        Country country = Country.fromIsoCode(value);
        if (country == null) {
          throw new IllegalArgumentException("Invalid country or country ISO code: " + value);
        }

        return country;
      case OCCURRENCE_ISSUE:
        return VocabularyUtils.lookup(value, OccurrenceIssue.class)
          .orElseThrow(() -> new IllegalArgumentException("Invalid occurrence issue: " + value));
      case TYPE_STATUS:
        return VocabularyUtils.lookup(value, TypeStatus.class)
          .orElseThrow(() -> new IllegalArgumentException("Invalid type status: " + value));
      case ENDPOINT_TYPE:
        return VocabularyUtils.lookup(value, EndpointType.class)
          .orElseThrow(() -> new IllegalArgumentException("Invalid protocol: " + value));
      case RANGE:
        try {
          if (value.contains(",")) {
            return new YearRange(value);
          } else {
            return Integer.parseInt(value.trim());
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid year value(s): " + value);
        }
      default:
        throw new IllegalArgumentException("Unsupported parameter type: " + type);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Parameter parameter = (Parameter) o;
    return name.equals(parameter.name) && value.equals(parameter.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }
}
