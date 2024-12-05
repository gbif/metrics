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
package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.ws.client.ClientBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceDatasetIndexWsClientIT {

  // should this change then we are really in trouble
  private static final int ANIMALIA_KEY = 1;
  private OccurrenceDatasetIndexService wsClient;

  @LocalServerPort
  int localServerPort;

  @BeforeEach
  public void init() {
    ClientBuilder clientBuilder = new ClientBuilder();
    wsClient =
        clientBuilder
            .withUrl("http://localhost:" + localServerPort)
            .build(OccurrenceDatasetIndexWsClient.class);
  }

  /** Ensures that the read works without throwing exception. */
  @Disabled
  @Test
  public void basicLookup() {
    wsClient.occurrenceDatasetsForNubKey(ANIMALIA_KEY);
  }
}
