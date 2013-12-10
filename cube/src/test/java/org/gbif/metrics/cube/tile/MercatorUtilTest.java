package org.gbif.metrics.cube.tile;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class MercatorUtilTest {

  @Test
  public void testGetOffset() {
    assertEquals(92, MercatorProjectionUtil.getOffsetY(45d, -90d, 0));
    assertEquals(128, MercatorProjectionUtil.getOffsetX(0d, 0d, 0));
    assertEquals(128, MercatorProjectionUtil.getOffsetY(0d, 0d, 0));
    assertEquals(0, MercatorProjectionUtil.getOffsetX(0d, 0d, 1));
    assertEquals(0, MercatorProjectionUtil.getOffsetY(0d, 0d, 1));
    assertEquals(0, MercatorProjectionUtil.getOffsetX(0d, 0d, 2));
    assertEquals(0, MercatorProjectionUtil.getOffsetY(0d, 0d, 2));

    // Canberra, AU
    double lng = 149.1;
    double lat = -35.2;
    assertEquals(0, MercatorProjectionUtil.toTileX(lng, 0));
    assertEquals(1, MercatorProjectionUtil.toTileX(lng, 1));
    assertEquals(3, MercatorProjectionUtil.toTileX(lng, 2));
    assertEquals(0, MercatorProjectionUtil.toTileY(lat, 0));
    assertEquals(1, MercatorProjectionUtil.toTileY(lat, 1));
    assertEquals(2, MercatorProjectionUtil.toTileY(lat, 2));
  }
}
