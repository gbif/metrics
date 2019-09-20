/**
 * 
 */
package org.gbif.metrics.cube;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.google.inject.BindingAnnotation;


/**
 * A disambiguation annotation to indicate that the annotated method expects to be dealing with the declared CubeIo.
 * Primarily used in conjunction with Guice providers.
 */
@BindingAnnotation
@Retention(RetentionPolicy.RUNTIME)
public @interface CubeIo {

  public enum Type {
    DENSITY_TILE, OCCURRENCE, TAXON_OCCURRENCE_DATASET, COUNTRY_OCCURRRENCE_DATASET
  }

  public abstract Type value();
}
