package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.Country;

import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;

public class CountryBucketer extends EnumToOrdinalBucketer<Country> {
  public static final int BYTES = 2;

  public CountryBucketer() {
    super(BYTES);
  }
}
