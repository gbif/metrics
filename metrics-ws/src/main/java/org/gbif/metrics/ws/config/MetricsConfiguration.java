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
package org.gbif.metrics.ws.config;

import org.gbif.metrics.MetricsService;
import org.gbif.metrics.es.EsConfig;
import org.gbif.metrics.es.EsMetricsService;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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

  @ConfigurationProperties(prefix = "cache")
  @Bean
  public EsMetricsService.CacheConfig cacheConfig() {
    return new EsMetricsService.CacheConfig();
  }

  @Bean
  public MetricsService metricsService(
      EsMetricsService.CacheConfig cacheConfig,
      @Value("${es.index}") String esIndex,
      ElasticsearchClient esClient,
      @Value("${defaultChecklistKey:d7dddbf4-2cf0-4f39-9b2a-bb099caae36c}")
          String defaultChecklistKey) {
    return new EsMetricsService(esIndex, cacheConfig, esClient, defaultChecklistKey);
  }

  /**
   * Owns sniffer + client so Spring can tear them down in the right order: sniffer first, then
   * {@link ElasticsearchClient#close()} (closes transport + underlying RestClient).
   */
  @Bean(destroyMethod = "close")
  public EsClientLifecycle esClientLifecycle(EsConfig esConfig) {
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

    RestClient restClient = builder.build();
    RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    ElasticsearchClient client = new ElasticsearchClient(transport);

    Sniffer sniffer = null;
    if (esConfig.getSniffInterval() > 0) {
      sniffer =
          Sniffer.builder(restClient)
              .setSniffIntervalMillis(esConfig.getSniffInterval())
              .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
              .build();
      sniffOnFailureListener.setSniffer(sniffer);
    }

    return new EsClientLifecycle(client, sniffer);
  }

  @Bean
  public ElasticsearchClient buildClient(EsClientLifecycle esClientLifecycle) {
    return esClientLifecycle.client();
  }

  /** Closes sniffer (if any) before the ES client/transport/RestClient. */
  static final class EsClientLifecycle implements Closeable {
    private final ElasticsearchClient client;
    private final Sniffer sniffer;

    EsClientLifecycle(ElasticsearchClient client, Sniffer sniffer) {
      this.client = client;
      this.sniffer = sniffer;
    }

    ElasticsearchClient client() {
      return client;
    }

    @Override
    public void close() throws IOException {
      if (sniffer != null) {
        sniffer.close();
      }
      // Closes transport and the underlying RestClient / HTTP connections.
      client.close();
    }
  }
}
