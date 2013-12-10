package org.gbif.metrics.ws.resources;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OccurrenceCubeResourceTest {

  @Test
  public void testSortDescending() {
    OccurrenceCubeResource r = new OccurrenceCubeResource(null, null, null);
    Map<CharSequence, Integer> m = Maps.newHashMap();
    m.put("82ec8f46-f762-11e1-a439-00145eb45e9a", 2571); // same value
    m.put("cf5ce014-6ac5-47fe-8b35-c70a9826562e", 2571); // same value
    m.put("12ec8f46-f762-11e1-a439-00145eb45e9a", 100000);

    Map<UUID, Integer> result = r.sortDescending(m);
    assertEquals(3, result.size());
    Iterator<Map.Entry<UUID, Integer>> iter = result.entrySet().iterator();
    assertEquals("12ec8f46-f762-11e1-a439-00145eb45e9a", iter.next().getKey().toString());
    assertEquals("82ec8f46-f762-11e1-a439-00145eb45e9a", iter.next().getKey().toString());
    assertEquals("cf5ce014-6ac5-47fe-8b35-c70a9826562e", iter.next().getKey().toString());
  }

  @Test
  public void testSortDescending2() {
    OccurrenceCubeResource r = new OccurrenceCubeResource(null, null, null);
    Map<Integer, Long> m = Maps.newHashMap();
    m.put(1, 2571l); // same value
    m.put(2, 2571l); // same value
    m.put(3, 100000l);

    Map<Integer, Long> result = r.sortDescendingValues(m);
    assertEquals(3, result.size());
    Iterator<Map.Entry<Integer, Long>> iter = result.entrySet().iterator();
    assertEquals((Integer) 3, iter.next().getKey());
    assertEquals((Integer) 1, iter.next().getKey());
    assertEquals((Integer) 2, iter.next().getKey());
  }
}
