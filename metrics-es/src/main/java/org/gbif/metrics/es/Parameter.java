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

import java.util.Objects;

/** Query parameter. */
public class Parameter {

  private final String name;
  private final String value;

  /**
   * @param name parameter name/key
   * @param value parameter value
   */
  public Parameter(String name, String value) {
    this.name = name;
    this.value = value;
  }

  /** @return parameter name/key */
  public String getName() {
    return name;
  }

  /** @return parameter value */
  public String getValue() {
    return value;
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
