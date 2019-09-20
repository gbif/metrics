package org.gbif.metrics.ws.guice;

import org.gbif.metrics.MetricsService;
import org.gbif.metrics.es.Config;
import org.gbif.metrics.es.EsMetricsService;

import java.util.Properties;

import com.google.inject.AbstractModule;

/**
 * Elasticsearch metrics module.
 */
class EsMetricsModule extends AbstractModule {


  private final Properties properties;

  EsMetricsModule(Properties properties) {
    this.properties = properties;
  }

  @Override
  protected void configure() {
    Config config = Config.from(Long.parseLong(properties.getProperty("cache.expire_after")),
                                properties.getProperty("es.index_name"),
                                properties.getProperty("es.hosts").split(","));
    bind(MetricsService.class).toInstance(new EsMetricsService(config));
  }
}
