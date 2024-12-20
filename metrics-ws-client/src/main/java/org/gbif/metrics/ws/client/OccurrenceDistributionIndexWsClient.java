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
package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDistributionIndexService;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Kingdom;

import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

/** Ws client for {@link OccurrenceDistributionIndexService}. */
public interface OccurrenceDistributionIndexWsClient extends OccurrenceDistributionIndexService {

  @Override
  default Map<BasisOfRecord, Long> getBasisOfRecordCounts() {
    Map<BasisOfRecord, Long> map =
        getBasisOfRecordCountsInternal().entrySet().stream()
            .collect(Collectors.toMap(e -> BasisOfRecord.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @GetMapping(
      value = "occurrence/counts/basisOfRecord",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getBasisOfRecordCountsInternal();

  @Override
  default Map<Kingdom, Long> getKingdomCounts() {
    Map<Kingdom, Long> map =
        getKingdomCountsInternal().entrySet().stream()
            .collect(Collectors.toMap(e -> Kingdom.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @GetMapping(value = "occurrence/counts/kingdom", produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getKingdomCountsInternal();

  @Override
  default Map<Integer, Long> getYearCounts(int from, int to) {
    String year = from + "," + to;
    return getYearCounts(year).entrySet().stream()
        .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey()), Map.Entry::getValue));
  }

  @GetMapping(value = "occurrence/counts/year", produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getYearCounts(@RequestParam(value = "year", required = false) String year);

  static <T extends Comparable<T>> SortedMap<T, Long> sortResponse(Map<T, Long> map) {
    return ImmutableSortedMap.copyOf(
        map,
        Ordering.natural()
            .onResultOf(Functions.forMap(map))
            .compound(Ordering.natural())
            .reverse());
  }
}
