package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.occurrencestore.util.BasisOfRecordConverter;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.bucketers.AbstractIdentityBucketer;
import com.urbanairship.datacube.serializables.IntSerializable;

/**
 * Bucketer that uses the basis of record converter to serialize as Ints
 */
public class BasisOfRecordBucketer extends AbstractIdentityBucketer<BasisOfRecord> {
  // The number of bytes this buckets into
  public static final int BYTES=4;

  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();

  @Override
  public CSerializable makeSerializable(BasisOfRecord bor) {
    return new IntSerializable(BOR_CONVERTER.fromEnum(bor));
  }
}
