/**
 * 
 */
package org.gbif.metrics.cube.tile.density;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.metrics.cube.mapred.OccurrenceWritable;


/**
 * Defines the layers (grids) that a density tile may hold.
 * [Future selves of us might question why we only go up to 2020, but unlikely we will use this code base in 2020.]
 */
public enum Layer {
  // Specimens
  SP_NO_YEAR, SP_PRE_1900, SP_1900_1910, SP_1910_1920, SP_1920_1930, SP_1930_1940, SP_1940_1950, SP_1950_1960,
  SP_1960_1970, SP_1970_1980, SP_1980_1990, SP_1990_2000, SP_2000_2010,
  SP_2010_2020,

  // Observations
  OBS_NO_YEAR, OBS_PRE_1900, OBS_1900_1910, OBS_1910_1920, OBS_1920_1930, OBS_1930_1940, OBS_1940_1950, OBS_1950_1960,
  OBS_1960_1970, OBS_1970_1980, OBS_1980_1990, OBS_1990_2000, OBS_2000_2010, OBS_2010_2020,

  // Living (data does not support time series)
  LIVING,

  // Fossils (data does not support time series)
  FOSSIL,

  // Other basis of record catches a lot of things (so year makes sense still)
  OTH_NO_YEAR, OTH_PRE_1900, OTH_1900_1910, OTH_1910_1920, OTH_1920_1930, OTH_1930_1940, OTH_1940_1950, OTH_1950_1960,
  OTH_1960_1970, OTH_1970_1980, OTH_1980_1990, OTH_1990_2000, OTH_2000_2010, OTH_2010_2020;

  // utility arrays used in inference only.
  // arrays ordering is tightly coupled to the inference code for the year below.
  private final static Layer[] SPECIMENS = {SP_NO_YEAR, SP_PRE_1900, SP_1900_1910, SP_1910_1920, SP_1920_1930,
    SP_1930_1940, SP_1940_1950, SP_1950_1960, SP_1960_1970, SP_1970_1980, SP_1980_1990, SP_1990_2000, SP_2000_2010,
    SP_2010_2020};
  private final static Layer[] OBSERVATIONS = {OBS_NO_YEAR, OBS_PRE_1900, OBS_1900_1910, OBS_1910_1920, OBS_1920_1930,
    OBS_1930_1940, OBS_1940_1950, OBS_1950_1960, OBS_1960_1970, OBS_1970_1980, OBS_1980_1990, OBS_1990_2000,
    OBS_2000_2010, OBS_2010_2020};
  private final static Layer[] OTHERS = {OTH_NO_YEAR, OTH_PRE_1900, OTH_1900_1910, OTH_1910_1920, OTH_1920_1930,
    OTH_1930_1940, OTH_1940_1950, OTH_1950_1960, OTH_1960_1970, OTH_1970_1980, OTH_1980_1990, OTH_1990_2000,
    OTH_2000_2010, OTH_2010_2020};

  /**
   * Utility to determine to which layer an occurrence record belongs.
   * This assumes that the record is plottable, so no verification on the coordinates is performed.
   * 
   * @param bor The basis of record for the Occurrence
   * @param year The year for the Occurrence
   * @return The layer that a record should belong to.
   */
  public static Layer inferFrom(BasisOfRecord bor, Integer year) {
    switch (bor) {
      case FOSSIL_SPECIMEN:
        return FOSSIL;
      case LIVING_SPECIMEN:
        return LIVING;

      case PRESERVED_SPECIMEN:
        return inferYear(year, SPECIMENS);

      case OBSERVATION:
      case HUMAN_OBSERVATION:
      case MACHINE_OBSERVATION:
        return inferYear(year, OBSERVATIONS);

      default:
        return inferYear(year, OTHERS);
    }
  }

  /**
   * Utility to determine to which layer an occurrence record belongs.
   * This assumes that the record is plottable, so no verification on the coordinates is performed.
   * 
   * @param o The Occurrence record which needs to be placed.
   * @return The layer that a record should belong to.
   */
  public static Layer inferFrom(OccurrenceWritable o) {
    return inferFrom(o.getBasisOfRecord(), o.getYear());
  }


  /**
   * Given the choices which are of a careful ordering (see definitions above) returns the
   * correct year.
   */
  private static Layer inferYear(Integer year, Layer[] choices) {
    if (year == null) {
      return choices[0];
    }

    if (year < 1900) {
      return choices[1];
    }

    int index = 2;
    for (int decade = 1900; decade <= 2010; decade += 10, index += 1) {
      if (year >= decade && year < decade + 10) {
        return choices[index];
      }
    }

    // could be post 2020 for example
    return choices[0];
  }
}
