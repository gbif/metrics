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
package org.gbif.metrics;

import org.gbif.metrics.es.AggregationQuery;
import org.gbif.metrics.es.CountQuery;

/** Management operations to flush and refresh cache entries. */
public interface MetricsCacheService {

  /** Flush the entries of all underlying caches. */
  void flush();

  /**
   * Evict and refresh a count query cached result.
   *
   * @param countQuery query to be refreshed
   */
  void refresh(CountQuery countQuery);

  /**
   * Evict and refresh a aggregation query cached result.
   *
   * @param aggregationQuery query to be refreshed
   */
  void refresh(AggregationQuery aggregationQuery);
}
