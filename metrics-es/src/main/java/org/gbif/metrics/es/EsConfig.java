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
package org.gbif.metrics.es;

public class EsConfig {

  // defaults
  private static final int CONNECT_TIMEOUT_DEFAULT = 6000;
  private static final int SOCKET_TIMEOUT_DEFAULT = 100000;
  private static final int SNIFF_INTERVAL_DEFAULT = 600000;
  private static final int SNIFF_AFTER_FAILURE_DELAY_DEFAULT = 60000;

  private String[] hosts;
  private String index;
  private int connectTimeout = CONNECT_TIMEOUT_DEFAULT;
  private int socketTimeout = SOCKET_TIMEOUT_DEFAULT;
  private int sniffInterval = SNIFF_INTERVAL_DEFAULT;
  private int sniffAfterFailureDelay = SNIFF_AFTER_FAILURE_DELAY_DEFAULT;

  public EsConfig() {
    super();
  }

  private EsConfig(
      String[] hosts,
      String index,
      int connectTimeout,
      int socketTimeout,
      int sniffInterval,
      int sniffAfterFailureDelay) {
    this.hosts = hosts;
    this.index = index;
    this.connectTimeout = connectTimeout;
    this.socketTimeout = socketTimeout;
    this.sniffInterval = sniffInterval;
    this.sniffAfterFailureDelay = sniffAfterFailureDelay;
  }

  public String[] getHosts() {
    return hosts;
  }

  public void setHosts(String[] hosts) {
    this.hosts = hosts;
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public void setConnectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public void setSocketTimeout(int socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public int getSniffInterval() {
    return sniffInterval;
  }

  public void setSniffInterval(int sniffInterval) {
    this.sniffInterval = sniffInterval;
  }

  public int getSniffAfterFailureDelay() {
    return sniffAfterFailureDelay;
  }

  public void setSniffAfterFailureDelay(int sniffAfterFailureDelay) {
    this.sniffAfterFailureDelay = sniffAfterFailureDelay;
  }
}
