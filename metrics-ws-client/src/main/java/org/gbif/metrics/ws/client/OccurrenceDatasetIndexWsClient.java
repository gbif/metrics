package org.gbif.metrics.ws.client;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.api.vocabulary.Country;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A web service client to support the accession of occurrence dataset indexes.
 */
public interface OccurrenceDatasetIndexWsClient extends OccurrenceDatasetIndexService {

  @Override
  default SortedMap<UUID, Long> occurrenceDatasetsForCountry(Country country) {
    Map<UUID, Long> datasets = getDatasets(country.getIso2LetterCode(), null)
        .entrySet().stream()
        .collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), Map.Entry::getValue));
    return sortResponse(datasets);
  }

  @Override
  default SortedMap<UUID, Long> occurrenceDatasetsForNubKey(int nubKey) {
    Map<UUID, Long> datasets = getDatasets(null, nubKey)
        .entrySet().stream()
        .collect(Collectors.toMap(e -> UUID.fromString(e.getKey()), Map.Entry::getValue));
    return sortResponse(datasets);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/datasets",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getDatasets(
      @RequestParam(value = "country", required = false) String country,
      @RequestParam(value = "nubKey", required = false) Integer nubKey);

  /**
   * Sorts map in reverse order.
   */
  static SortedMap<UUID, Long> sortResponse(Map<UUID, Long> map) {
    return ImmutableSortedMap.copyOf(map,
        Ordering.natural().onResultOf(Functions.forMap(map)).compound(Ordering.natural()).reverse());
  }
}
