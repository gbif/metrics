package org.gbif.metrics.cube.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

public abstract class SerDeUtils {

  private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  protected static <T> T decodeObject(final T object, final byte[] data, final SpecificDatumReader<T> reader) throws IOException {
    Decoder decoder = DECODER_FACTORY.binaryDecoder(data, null);
    return reader.read(object, decoder);
  }

  protected static <T> byte[] encodeObject(final T datum, final GenericDatumWriter<T> writer) throws IOException {
    // The encoder instantiation can be replaced with a ThreadLocal if needed
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    BinaryEncoder encoder = ENCODER_FACTORY.binaryEncoder(os, null);
    writer.write(datum, encoder);
    encoder.flush();
    return os.toByteArray();
  }
}
