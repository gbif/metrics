package org.gbif.metrics.cube.tile.io;

import org.gbif.metrics.cube.util.SerDeUtils;

import java.io.IOException;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class LocationsAvroSerDeUtils extends SerDeUtils {

  private static final GenericDatumWriter<LocationsAvro> WRITER = new SpecificDatumWriter<LocationsAvro>(LocationsAvro.class);
  private static final SpecificDatumReader<LocationsAvro> READER = new SpecificDatumReader<LocationsAvro>(LocationsAvro.class);

  public static LocationsAvro decode(final byte[] data) throws IOException {
    return decodeObject(new LocationsAvro(), data, READER);
  }

  public static byte[] encode(final LocationsAvro l) throws IOException {
    return encodeObject(l, WRITER);
  }
}
