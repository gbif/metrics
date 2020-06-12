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

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Query to obtain simple occurrence counts based on a list of parameters. */
public class CountQuery {

  private final Set<Parameter> parameters;

  /**
   * Full constructors.
   *
   * @param parameters list of parameters.
   */
  public CountQuery(Set<Parameter> parameters) {
    this.parameters = parameters;
  }

  /** Creates an instance with an initial list of empty parameters. */
  public CountQuery() {
    this.parameters = new HashSet<>();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CountQuery that = (CountQuery) o;
    return that.parameters.equals(this.parameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parameters);
  }

  /** @return list of parameters used by the count query */
  public Set<Parameter> getParameters() {
    return parameters;
  }

  /**
   * Adds a parameter to the list and return this instance to make this a chainable call.
   *
   * @param name parameter key/name
   * @param value parameter value
   * @return current instance
   */
  public CountQuery withParameter(String name, String value) {
    parameters.add(new Parameter(name, value));
    return this;
  }
}
