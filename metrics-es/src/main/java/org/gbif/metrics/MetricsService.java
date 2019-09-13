package org.gbif.metrics;

import org.gbif.metrics.es.AggregationQuery;
import org.gbif.metrics.es.CountQuery;

import java.util.Map;

/**
 * Provides basic counts and aggregations of occurrence records on multiple dimensions.
 */
public interface MetricsService {

  /**
   * Provides a single count of occurrence records from a list of filters.
   * @param countQuery query/filters
   * @return total number of occurrence records
   */
  Long count(CountQuery countQuery);

  /**
   * Provides a simple service with counts of multiple dimensions/fields.
   * @param aggregationQuery query containing the requested aggregation
   * @return a simple map of dimensions/labels and counts
   */
  Map<String, Long> countAggregation(AggregationQuery aggregationQuery);
}
