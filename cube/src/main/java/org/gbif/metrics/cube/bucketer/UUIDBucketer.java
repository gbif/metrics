package org.gbif.metrics.cube.bucketer;

import java.util.UUID;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.bucketers.AbstractIdentityBucketer;
import com.urbanairship.datacube.serializables.BytesSerializable;

/**
 * Bucketer that serializes UUIDs as 36 bytes of text.
 */
public class UUIDBucketer extends AbstractIdentityBucketer<UUID> {
  // UUID are 36 long
  public static final int BYTES=36;

  @Override
  public CSerializable makeSerializable(UUID coord) {
    return new BytesSerializable(coord.toString().getBytes());
  }
}
