package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.EndpointType;

import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;

public class EndpointTypeBucketer extends EnumToOrdinalBucketer<EndpointType> {
  public static final int BYTES = 1;

  public EndpointTypeBucketer() {
    super(BYTES);
  }
}
