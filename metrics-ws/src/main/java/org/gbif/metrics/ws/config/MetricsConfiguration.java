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
import org.gbif.metrics.es.EsConfig;
import org.gbif.metrics.es.EsMetricsService;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfiguration {

  @ConfigurationProperties(prefix = "es")
  @Bean
  public EsConfig esConfig() {
    return new EsConfig();
  }

  @Bean
  public MetricsService metricsService(
      @Value("${cache.expire_after}") String expireAfter,
      @Value("${es.index}") String esIndex,
      RestHighLevelClient esClient) {
    return new EsMetricsService(esIndex, Long.parseLong(expireAfter), esClient);
  }

  @Bean
  public RestHighLevelClient buildClient(EsConfig esConfig) {
    HttpHost[] hosts = new HttpHost[esConfig.getHosts().length];
    int i = 0;
    for (String host : esConfig.getHosts()) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();

    RestClientBuilder builder =
        RestClient.builder(hosts)
            .setRequestConfigCallback(
                requestConfigBuilder ->
                    requestConfigBuilder
                        .setConnectTimeout(esConfig.getConnectTimeout())
                        .setSocketTimeout(esConfig.getSocketTimeout()))
            .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

    if (esConfig.getSniffInterval() > 0) {
      builder.setFailureListener(sniffOnFailureListener);
    }

    RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

    if (esConfig.getSniffInterval() > 0) {
      Sniffer sniffer =
          Sniffer.builder(highLevelClient.getLowLevelClient())
              .setSniffIntervalMillis(esConfig.getSniffInterval())
              .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
              .build();
      sniffOnFailureListener.setSniffer(sniffer);

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    sniffer.close();
                    try {
                      highLevelClient.close();
                    } catch (IOException e) {
                      throw new IllegalStateException("Couldn't close ES client", e);
                    }
                  }));
    }
    return highLevelClient;
  }
}
