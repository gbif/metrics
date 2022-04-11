/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.metrics.ws.resources;

import java.util.Calendar;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Range;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OccurrenceCubeResourceTest {

  private static final int CURRENT_YEAR = Calendar.getInstance().get(Calendar.YEAR);

  @Test
  public void testYearRange() {
    assertRange(OccurrenceCubeResource.parseYearRange("1742"), 1742, CURRENT_YEAR + 1);
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
    assertThrows(
        IllegalArgumentException.class, () -> OccurrenceCubeResource.parseYearRange(years));
  }
}
