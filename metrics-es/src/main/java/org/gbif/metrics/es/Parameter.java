package org.gbif.metrics.es;

import java.util.Objects;

/**
 * Query parameter.
 */
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

  /**
   * @return parameter name/key
   */
  public String getName() {
    return name;
  }

  /**
   * @return parameter value
   */
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
