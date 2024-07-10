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
package org.gbif.metrics.es;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class YearRange {
  private final int startYear;
  private final int endYear;

  public YearRange(String rangeString) {
    String[] parts = rangeString.split(",");
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid range format: " + rangeString);
    }
    int start = Integer.parseInt(parts[0].trim());
    int end = Integer.parseInt(parts[1].trim());
    validateRange(start, end);
    this.startYear = start;
    this.endYear = end;
  }

  private void validateRange(int start, int end) {
    if (start > end) {
      throw new IllegalArgumentException("Invalid year range: " + start + "," + end);
    }
  }
}
