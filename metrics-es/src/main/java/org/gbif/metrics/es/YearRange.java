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
