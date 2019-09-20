package org.gbif.metrics.cube.index.common;

import org.gbif.metrics.cube.util.GenericStringIntMapDatumReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Flushables;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;


/**
 * A simple data structure that can serialize and deserialize itself efficiently using Avro.
 * This implements the the Op interface to support its use as the target of an address in a datacube,
 * in addition to a nested deserializer. For safety, this ensures that the integer is always positive
 * thus any attempts to subtract which produce negative numbers are considered illegal and the map
 * will remain at 0.
 */
public class UuidIntMap implements Op {

  /**
   * Suitable for use in cube definitions, deserializes using avro.
   */
  public static class CountByDatasetDeserializer implements Deserializer<UuidIntMap> {

    @Override
    public UuidIntMap fromBytes(byte[] bytes) {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      try {
        GenericStringIntMapDatumReader reader = new GenericStringIntMapDatumReader(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
        Map<CharSequence, Integer> r = reader.read(null, decoder);
        return new UuidIntMap(r);
      } catch (IOException e) {
        throw new RuntimeException("Unable to deserialize UuidIntMap: " + e.getMessage());
      }
    }
  }

  public static final CountByDatasetDeserializer DESERIALIZER = new CountByDatasetDeserializer();

  public static final UuidIntMap EMPTY_MAP = new UuidIntMap(ImmutableMap.<CharSequence, Integer>of());
  private static final Schema schema = Schema.createMap(Schema.create(Type.INT));

  // CharSequence due to Avro format
  private final Map<CharSequence, Integer> counts;

  /**
   * Internal constructor for use with the format used by Avro.
   * Prefer the static factory over this.
   */
  @VisibleForTesting
  UuidIntMap(Map<CharSequence, Integer> counts) {
    this.counts = ImmutableMap.copyOf(counts); // defensive copy
  }

  /**
   * The preferred static factory to use.
   */
  public static UuidIntMap newInstance(Map<UUID, Integer> counts) {
    Map<CharSequence, Integer> c = Maps.newHashMap();
    for (Entry<UUID, Integer> e : counts.entrySet()) {
      c.put(e.getKey().toString(), e.getValue());
    }
    return new UuidIntMap(c);
  }

  private static Map<CharSequence, Integer> toCharsequenceIntMap(Map<Utf8, Integer> input) {
    ImmutableMap.Builder<CharSequence, Integer> builder = new ImmutableMap.Builder<CharSequence, Integer>();
    for (Entry<Utf8, Integer> entry : input.entrySet()) {
      builder.put(entry.getKey().toString(), entry.getValue());
    }
    return builder.build();
  }

  /**
   * Merges the mapped counts in the other op.
   */
  @Override
  public UuidIntMap add(Op otherOp) {
    Preconditions.checkArgument(otherOp instanceof UuidIntMap, "Can only add type UuidIntMap");
    UuidIntMap o = (UuidIntMap) otherOp;
    return new UuidIntMap(merge(counts, o.getCounts(), false));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof UuidIntMap)) {
      return false;
    }
    UuidIntMap other = (UuidIntMap) obj;
    // we don't care on ordering, so we just need to ensure there are no
    // entries that differ
    return counts != null && other.counts != null && Maps.difference(counts, other.counts).entriesDiffering().isEmpty();
  }

  public Map<CharSequence, Integer> getCounts() {
    return counts;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(counts);
  }

  /**
   * Serializes using avro.
   */
  @Override
  public byte[] serialize() throws RuntimeException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      Encoder e = EncoderFactory.get().binaryEncoder(baos, null);
      GenericDatumWriter<Map<CharSequence, Integer>> writer =
        new GenericDatumWriter<Map<CharSequence, Integer>>(schema);
      writer.write(counts, e);
      Flushables.flushQuietly(e);
      return baos.toByteArray();
    } catch (IOException e) {
      // We can't throw a checked exception, so repackage
      throw new IllegalStateException("Unable to serialize: " + e);
    }
  }

  /**
   * Merges the mapped counts in the other op.
   */
  @Override
  public UuidIntMap subtract(Op otherOp) {
    Preconditions.checkArgument(otherOp != null && otherOp instanceof UuidIntMap, "Can only subtract type UuidIntMap");
    UuidIntMap o = (UuidIntMap) otherOp;
    return new UuidIntMap(merge(counts, o.getCounts(), true));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("counts", counts)
      .toString();
  }

  /**
   * Subtraction aware merge.
   *
   * @param m1 The source
   * @param m2 To be merged into m1
   * @param isSubtract If true, m2 is subtracted from m1
   */
  private Map<CharSequence, Integer> merge(Map<CharSequence, Integer> m1,
    Map<CharSequence, Integer> m2, boolean isSubtract) {
    Map<CharSequence, Integer> source = Maps.newHashMap(m1);
    for (Entry<CharSequence, Integer> e : m2.entrySet()) {
      CharSequence k = e.getKey();
      int i = e.getValue();
      if (isSubtract) {
        if (source.containsKey(k)) {
          i = source.get(k) - i;
        } else {
          continue; // ignore subtractions if not in the source
        }
      } else {
        i = source.containsKey(k) ? source.get(k) + i : i;
      }
      i = i < 0 ? 0 : i; // ensure we don't go below 0
      source.put(k, i);
    }
    return ImmutableMap.copyOf(source);
  }
}
