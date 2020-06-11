package org.gbif.metrics.ws.config;

import org.gbif.metrics.MetricsService;
import org.gbif.metrics.es.Config;
import org.gbif.metrics.es.EsMetricsService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// TODO: 11/06/2020 servlet listener
@Configuration
public class MetricsConfiguration {

  @Bean
  public MetricsService metricsService(
      @Value("${cache.expire_after}") String expireAfter,
      @Value("${es.index_name}") String indexName,
      @Value("${hosts}") String hosts) {
    Config config = Config.from(Long.parseLong(expireAfter), indexName, hosts.split(","));
    return new EsMetricsService(config);
  }
}
