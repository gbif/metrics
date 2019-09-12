package org.gbif.metrics.ws.guice;

import org.gbif.metrics.es.EsConfig;
import org.gbif.metrics.es.EsMetricsService;

import java.util.Properties;

import com.google.inject.AbstractModule;

class EsMetricsModule extends AbstractModule {


  private final Properties properties;

  EsMetricsModule(Properties properties) {
    this.properties = properties;
  }

  @Override
  protected void configure() {
    EsConfig esConfig = EsConfig.from(properties.getProperty("es.index_name"), properties.getProperty("es.hosts").split(","));
    bind(EsMetricsService.class).toInstance(new EsMetricsService(esConfig));
  }
}
