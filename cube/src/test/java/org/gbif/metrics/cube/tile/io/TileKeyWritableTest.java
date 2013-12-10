package org.gbif.metrics.cube.tile.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TileKeyWritableTest {

  @Test
  public void testHashCode() {
    TileKeyWritable a = new TileKeyWritable(TileContentType.TAXON, "1", 1, 1, 1);
    TileKeyWritable b = new TileKeyWritable(TileContentType.TAXON, "1", 1, 1, 1);
    TileKeyWritable c = new TileKeyWritable(TileContentType.ALL, "1", 1, 1, 1);
    assertTrue(a.hashCode() == b.hashCode());
    assertFalse(a.hashCode() == c.hashCode());
    assertEquals(a.getType().hashCode(), b.getType().hashCode());
  }

}
