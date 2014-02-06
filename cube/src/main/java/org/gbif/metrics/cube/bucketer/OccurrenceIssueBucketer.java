package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.OccurrenceIssue;

import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;

public class OccurrenceIssueBucketer extends EnumToOrdinalBucketer<OccurrenceIssue> {
  public static final int BYTES = 1;

  public OccurrenceIssueBucketer() {
    super(BYTES);
  }
}
