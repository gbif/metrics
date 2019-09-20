package org.gbif.metrics.cube.tile.bucketer;

import com.google.common.base.Strings;
import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.bucketers.AbstractIdentityBucketer;
import com.urbanairship.datacube.serializables.StringSerializable;

/**
 * Encodes and decodes the various map keys into 36 character bytes padding with spaces.
 * 36 characters are required for UUIDs which are considered the longest keys supported by maps.
 */
public class MapKeyBucketer extends AbstractIdentityBucketer<String> {
  private static final MapKeyBucketer instance = new MapKeyBucketer();
  public static final int BYTES=36;

  @Override
  public CSerializable makeSerializable(String coordinateField) {
    return new StringSerializable(Strings.padStart(coordinateField, 36, ' '));
  }

  public static final MapKeyBucketer getInstance() {
    return instance;
  }
}
