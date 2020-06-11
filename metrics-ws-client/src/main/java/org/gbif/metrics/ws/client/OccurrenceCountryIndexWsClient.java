package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceCountryIndexService;
import org.gbif.api.vocabulary.Country;

import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * A web service client to support the accession of occurrence dataset indexes.
 */
public interface OccurrenceCountryIndexWsClient extends OccurrenceCountryIndexService {

  @Override
  default Map<Country, Long> publishingCountriesForCountry(Country country) {
    Map<Country, Long> map = getPublishingCountries(country.getIso2LetterCode())
        .entrySet().stream()
        .collect(Collectors.toMap(e -> Country.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/publishingCountries",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getPublishingCountries(@RequestParam(value = "country", required = false) String country);

  @Override
  default Map<Country, Long> countriesForPublishingCountry(Country publishingCountry) {
    Map<Country, Long> map = getCountries(publishingCountry.getIso2LetterCode())
        .entrySet().stream()
        .collect(Collectors.toMap(e -> Country.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/countries",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getCountries(@RequestParam(value = "publishingCountry", required = false) String publishingCountry);

  /**
   * Sorts map in reverse order.
   */
  static Map<Country, Long> sortResponse(Map<Country, Long> map) {
    return ImmutableSortedMap.copyOf(map,
      Ordering.natural().onResultOf(Functions.forMap(map)).compound(Ordering.natural()).reverse());
  }
}
