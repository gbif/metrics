package org.gbif.metrics.cube.tile.density.io;

import org.gbif.metrics.cube.util.SerDeUtils;

import java.io.IOException;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class DensityTileAvroSerDeUtils extends SerDeUtils {

  private static final GenericDatumWriter<DensityTileAvro> WRITER = new SpecificDatumWriter<DensityTileAvro>(DensityTileAvro.class);
  private static final SpecificDatumReader<DensityTileAvro> READER = new SpecificDatumReader<DensityTileAvro>(DensityTileAvro.class);

  public static DensityTileAvro decode(final byte[] data) throws IOException {
    return decodeObject(new DensityTileAvro(), data, READER);
  }

  public static byte[] encode(final DensityTileAvro tile) throws IOException {
    return encodeObject(tile, WRITER);
  }
}
