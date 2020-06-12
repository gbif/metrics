/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
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

import org.gbif.api.service.occurrence.OccurrenceCountryIndexService;
import org.gbif.api.vocabulary.Country;

import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;

/** A web service client to support the accession of occurrence dataset indexes. */
public interface OccurrenceCountryIndexWsClient extends OccurrenceCountryIndexService {

  @Override
  default Map<Country, Long> publishingCountriesForCountry(Country country) {
    Map<Country, Long> map =
        getPublishingCountries(country.getIso2LetterCode()).entrySet().stream()
            .collect(Collectors.toMap(e -> Country.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/publishingCountries",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getPublishingCountries(
      @RequestParam(value = "country", required = false) String country);

  @Override
  default Map<Country, Long> countriesForPublishingCountry(Country publishingCountry) {
    Map<Country, Long> map =
        getCountries(publishingCountry.getIso2LetterCode()).entrySet().stream()
            .collect(Collectors.toMap(e -> Country.valueOf(e.getKey()), Map.Entry::getValue));
    return sortResponse(map);
  }

  @RequestMapping(
      method = RequestMethod.GET,
      value = "occurrence/counts/countries",
      produces = MediaType.APPLICATION_JSON_VALUE)
  Map<String, Long> getCountries(
      @RequestParam(value = "publishingCountry", required = false) String publishingCountry);

  /** Sorts map in reverse order. */
  static Map<Country, Long> sortResponse(Map<Country, Long> map) {
    return ImmutableSortedMap.copyOf(
        map,
        Ordering.natural()
            .onResultOf(Functions.forMap(map))
            .compound(Ordering.natural())
            .reverse());
  }
}
