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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
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

}
