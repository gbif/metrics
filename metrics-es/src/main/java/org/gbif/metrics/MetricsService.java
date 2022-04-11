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

import java.util.Map;

/** Provides basic counts and aggregations of occurrence records on multiple dimensions. */
public interface MetricsService {

  /**
   * Provides a single count of occurrence records from a list of filters.
   *
   * @param countQuery query/filters
   * @return total number of occurrence records
   */
  Long count(CountQuery countQuery);

  /**
   * Provides a simple service with counts of multiple dimensions/fields.
   *
   * @param aggregationQuery query containing the requested aggregation
   * @return a simple map of dimensions/labels and counts
   */
  Map<String, Long> countAggregation(AggregationQuery aggregationQuery);
}
