package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.ws.client.ClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceDatasetIndexWsClientIT {

  // should this change then we are really in trouble
  private static final int ANIMALIA_KEY = 1;
  private OccurrenceDatasetIndexService wsClient;

  @LocalServerPort
  int localServerPort;

  @BeforeEach
  public void init() {
    ClientFactory clientFactory = new ClientFactory("http://localhost:" + localServerPort);
    wsClient = clientFactory.newInstance(OccurrenceDatasetIndexWsClient.class);
  }

  /**
   * Ensures that the read works without throwing exception.
   */
  @Test
  public void basicLookup() {
    wsClient.occurrenceDatasetsForNubKey(ANIMALIA_KEY);
  }
}
