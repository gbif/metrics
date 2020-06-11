package org.gbif.metrics.ws.client;

import org.gbif.api.service.metrics.CubeService;
import org.gbif.ws.client.ClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CubeWsClientIT {

  private CubeService wsClient;

  @LocalServerPort
  int localServerPort;

  @BeforeEach
  public void init() {
    ClientFactory clientFactory = new ClientFactory("http://localhost:" + localServerPort);
    wsClient = clientFactory.newInstance(CubeWsClient.class);
  }

  /**
   * An IT to simply check the scheme can be read, and that some rollups exist.
   * Is not meant to test any business logic.
   */
  @Test
  public void schema() {
    assertTrue(wsClient.getSchema().size() > 0, "CubeIo schema says no rollups which can't be true");
  }
}
