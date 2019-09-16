package org.gbif.metrics;

import org.gbif.metrics.es.AggregationQuery;
import org.gbif.metrics.es.CountQuery;

/**
 * Management operations to flush and refresh cache entries.
 */
public interface MetricsCacheService {

  /**
   * Flush the entries of all underlying caches.
   */
  void flush();

  /**
   * Evict and refresh a count query cached result.
   * @param countQuery query to be refreshed
   */
  void refresh(CountQuery countQuery);

  /**
   * Evict and refresh a aggregation query cached result.
   * @param aggregationQuery query to be refreshed
   */
  void refresh(AggregationQuery aggregationQuery);
}
