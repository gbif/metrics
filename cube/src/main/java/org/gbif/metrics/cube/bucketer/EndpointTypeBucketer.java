package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.EndpointType;
import org.gbif.occurrencestore.util.EndpointTypeConverter;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.bucketers.AbstractIdentityBucketer;
import com.urbanairship.datacube.serializables.IntSerializable;

/**
 * Bucketer that serializes instances of EndpointType.
 */
public class EndpointTypeBucketer extends AbstractIdentityBucketer<EndpointType> {

  // The number of bytes this buckets into
  public static final int BYTES = 4;

  private static final EndpointTypeConverter CONVERTER = new EndpointTypeConverter();

  @Override
  public CSerializable makeSerializable(EndpointType endpointType) {
    return new IntSerializable(CONVERTER.fromEnum(endpointType));
  }
}
