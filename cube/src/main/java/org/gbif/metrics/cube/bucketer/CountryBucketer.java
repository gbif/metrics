package org.gbif.metrics.cube.bucketer;

import org.gbif.api.vocabulary.Country;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.bucketers.AbstractIdentityBucketer;
import com.urbanairship.datacube.serializables.BytesSerializable;

/**
 * Bucketer that serializes as 2 letter ISO codes.
 */
public class CountryBucketer extends AbstractIdentityBucketer<Country> {
  // The number of bytes this buckets into (2 letter ISO codes)
  public static final int BYTES=2;

  @Override
  public CSerializable makeSerializable(Country coord) {
    return new BytesSerializable(coord.getIso2LetterCode().getBytes());
  }
}
