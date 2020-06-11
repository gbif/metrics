package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDistributionIndexService;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Kingdom;

import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Ws client for {@link OccurrenceDistributionIndexService}.
 */
public interface OccurrenceDistributionIndexWsClient extends OccurrenceDistributionIndexService {

  @Override
  default Map<BasisOfRecord, Long> getBasisOfRecordCounts() {
    Map<BasisOfRecord, Long> map = getBasisOfRecordCountsInternal()
        .entrySet().stream()
        .collect(Collectors.toMap(e -> BasisOfRecord.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/basisOfRecord",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getBasisOfRecordCountsInternal();

  @Override
  default Map<Kingdom, Long> getKingdomCounts() {
    Map<Kingdom, Long> map = getKingdomCountsInternal()
        .entrySet().stream()
        .collect(Collectors.toMap(e -> Kingdom.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/kingdom",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getKingdomCountsInternal();

  @Override
  default Map<Integer, Long> getYearCounts(int from, int to) {
    String year = from + "," + to;
    return getYearCounts(year)
        .entrySet().stream()
        .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey()), Map.Entry::getValue));
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/year",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getYearCounts(@RequestParam(value = "year", required = false) String year);

  static <T extends Comparable<T>> SortedMap<T, Long> sortResponse(Map<T, Long> map) {
    return ImmutableSortedMap.copyOf(map,
        Ordering.natural().onResultOf(Functions.forMap(map)).compound(Ordering.natural()).reverse());
  }
}
