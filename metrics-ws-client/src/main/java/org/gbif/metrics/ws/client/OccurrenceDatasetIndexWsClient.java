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

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.api.vocabulary.Country;

import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

/** A web service client to support the accession of occurrence dataset indexes. */
public interface OccurrenceDatasetIndexWsClient extends OccurrenceDatasetIndexService {

  @Override
  default SortedMap<UUID, Long> occurrenceDatasetsForCountry(Country country) {
    Map<UUID, Long> datasets =
        getDatasets(country.getIso2LetterCode(), null).entrySet().stream()
            .collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), Map.Entry::getValue));
    return sortResponse(datasets);
  }

  @Override
  default SortedMap<UUID, Long> occurrenceDatasetsForNubKey(int nubKey) {
    Map<UUID, Long> datasets =
        getDatasets(null, nubKey).entrySet().stream()
            .collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), Map.Entry::getValue));
    return sortResponse(datasets);
  }

  @GetMapping(
      value = "occurrence/counts/datasets",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getDatasets(
      @RequestParam(value = "country", required = false) String country,
      @RequestParam(value = "nubKey", required = false) Integer nubKey);

  /** Sorts map in reverse order. */
  static SortedMap<UUID, Long> sortResponse(Map<UUID, Long> map) {
    return ImmutableSortedMap.copyOf(
        map,
        Ordering.natural()
            .onResultOf(Functions.forMap(map))
            .compound(Ordering.natural())
            .reverse());
  }
}
