package org.gbif.metrics.ws.resources;

import java.util.Calendar;

import com.google.common.collect.Range;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OccurrenceCubeResourceTest {

  private static final int CURRENT_YEAR = Calendar.getInstance().get(Calendar.YEAR);

  @Test
  public void testYearRange() {
    assertRange(OccurrenceCubeResource.parseYearRange("1742"), 1742, CURRENT_YEAR+1);
    assertRange(OccurrenceCubeResource.parseYearRange("1742,1802"), 1742, 1802);
    assertRange(OccurrenceCubeResource.parseYearRange(""), 1500, CURRENT_YEAR + 1);
    assertRange(OccurrenceCubeResource.parseYearRange(null), 1500, CURRENT_YEAR + 1);
    assertRange(OccurrenceCubeResource.parseYearRange("1500,1900"), 1500, 1900);
  }

  @Test
  public void testIllegalYearRange() {
    assertIllegalRange("12,13");
    assertIllegalRange("-321,1981");
    assertIllegalRange("1200,2100");
    assertIllegalRange("2040");
    assertIllegalRange("999");
  }


  private void assertRange(Range<Integer> range, Integer start, Integer end) {
    assertTrue(range.hasLowerBound());
    assertTrue(range.hasUpperBound());

    assertEquals(start, range.lowerEndpoint());
    assertEquals(end, range.upperEndpoint());
  }

  private void assertIllegalRange(String years) {
    try {
      OccurrenceCubeResource.parseYearRange(years);
      fail( years + " is an illegal range");
    } catch (IllegalArgumentException e) {

    }
  }

}
