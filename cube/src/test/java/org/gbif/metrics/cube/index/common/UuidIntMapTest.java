package org.gbif.metrics.cube.index.common;

import org.gbif.metrics.cube.index.common.UuidIntMap.CountByDatasetDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Basic tests to verify the counts increment correctly and SerDe works ok.
 */
public class UuidIntMapTest {

  @Test
  public void testIncrements() {
    Map<CharSequence, Integer> m = new HashMap<CharSequence, Integer>();
    m.put("1", 1);
    UuidIntMap o1 = new UuidIntMap(m);
    UuidIntMap o2 = new UuidIntMap(m);

    // basic addition
    UuidIntMap o3 = o1.add(o2);
    assertNotNull(o3);
    assertNotNull(o3.getCounts());
    assertEquals(1, o3.getCounts().size());
    assertEquals(2, (int) o3.getCounts().get("1"));

    // basic subtraction
    UuidIntMap o4 = o3.subtract(o2);
    assertNotNull(o4);
    assertNotNull(o4.getCounts());
    assertEquals(1, o4.getCounts().size());
    assertEquals(1, (int) o4.getCounts().get("1"));
    o4 = o4.subtract(o2);
    assertNotNull(o4);
    assertNotNull(o4.getCounts());
    assertEquals(1, o4.getCounts().size());
    assertEquals(0, (int) o4.getCounts().get("1"));
    o4 = o4.subtract(o2);
    assertNotNull(o4);
    assertNotNull(o4.getCounts());
    assertEquals(1, o4.getCounts().size());
    assertEquals(0, (int) o4.getCounts().get("1")); // ensure does not go negative
  }


  @Test
  public void testIncrementsWithUUIDs() {
    UUID randomUuid = UUID.randomUUID();
    Map<UUID, Integer> m = new HashMap<UUID, Integer>();
    m.put(randomUuid, 1);
    UuidIntMap o1 = UuidIntMap.newInstance(m);
    UuidIntMap o2 = UuidIntMap.DESERIALIZER.fromBytes(o1.serialize());

    // basic addition
    UuidIntMap o3 = o1.add(o2);
    assertNotNull(o3);
    assertNotNull(o3.getCounts());
    assertEquals(1, o3.getCounts().size());
    assertEquals(2, (int) o3.getCounts().get(randomUuid.toString()));

    // basic subtraction
    UuidIntMap o4 = o3.subtract(o2);
    assertNotNull(o4);
    assertNotNull(o4.getCounts());
    assertEquals(1, o4.getCounts().size());
    assertEquals(1, (int) o4.getCounts().get(randomUuid.toString()));
    o4 = o4.subtract(o2);
    assertNotNull(o4);
    assertNotNull(o4.getCounts());
    assertEquals(1, o4.getCounts().size());
    assertEquals(0, (int) o4.getCounts().get(randomUuid.toString()));
    o4 = o4.subtract(o2);
    assertNotNull(o4);
    assertNotNull(o4.getCounts());
    assertEquals(1, o4.getCounts().size());
    assertEquals(0, (int) o4.getCounts().get(randomUuid.toString())); // ensure does not go negative
  }

  @Test
  public void testSerDe() {
    Map<CharSequence, Integer> m = new HashMap<CharSequence, Integer>();
    m.put("1", 1);
    m.put("2", 2);
    UuidIntMap o1 = new UuidIntMap(m);
    byte[] b = o1.serialize();
    UuidIntMap o2 = new CountByDatasetDeserializer().fromBytes(b);
    assertEquals(o1.hashCode(), o2.hashCode());
    assertEquals(o1, o2);
  }

}
