package org.gbif.metrics.cube.util;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;


/**
 * Read data from a Avro Schema.
 * By defaults deserialized a Charsequence/String object into a String object.
 * The superclass GenericDatumReader deserializes Strings into Utf8 classes.
 */
public class GenericStringIntMapDatumReader extends GenericDatumReader<Map<CharSequence, Integer>> {

  public GenericStringIntMapDatumReader(Schema schema) {
    super(schema);
  }

  /**
   * Overrides the method GenericDatumReader.readString to produce a String instance instead of Utf8 instance.
   */
  @Override
  protected Object readString(Object old, Decoder in) throws IOException {
    return super.readString(old, in).toString();
  }
}
