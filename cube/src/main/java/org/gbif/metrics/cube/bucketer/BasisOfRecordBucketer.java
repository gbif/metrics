package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.BasisOfRecord;

import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;

public class BasisOfRecordBucketer extends EnumToOrdinalBucketer<BasisOfRecord> {
  public static final int BYTES = 1;

  public BasisOfRecordBucketer() {
    super(BYTES);
  }
}
