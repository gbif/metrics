package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.TypeStatus;

import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;

public class TypeStatusBucketer extends EnumToOrdinalBucketer<TypeStatus> {
  public static final int BYTES = 1;

  public TypeStatusBucketer() {
    super(BYTES);
  }
}
