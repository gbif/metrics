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
package org.gbif.metrics.ws.config;

import org.gbif.metrics.MetricsService;
import org.gbif.metrics.es.Config;
import org.gbif.metrics.es.EsMetricsService;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfiguration {

  @Bean
  public MetricsService metricsService(
      @Value("${cache.expire_after}") String expireAfter,
      @Value("${es.index_name}") String indexName,
      @Value("${es.hosts}") String hosts) {
    Config config = Config.from(Long.parseLong(expireAfter), indexName, hosts.split(","));
    return new EsMetricsService(config);
  }
}
