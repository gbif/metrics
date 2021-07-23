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

import org.gbif.api.service.metrics.CubeService;
import org.gbif.ws.client.ClientBuilder;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CubeWsClientIT {

  private CubeService wsClient;

  @LocalServerPort int localServerPort;

  @BeforeEach
  public void init() {
    ClientBuilder clientBuilder = new ClientBuilder();
    wsClient =
        clientBuilder.withUrl("http://localhost:" + localServerPort).build(CubeWsClient.class);
  }

  /**
   * An IT to simply check the scheme can be read, and that some rollups exist. Is not meant to test
   * any business logic.
   */
  @Disabled
  @Test
  public void schema() {
    assertTrue(
        wsClient.getSchema().size() > 0, "CubeIo schema says no rollups which can't be true");
  }
}
