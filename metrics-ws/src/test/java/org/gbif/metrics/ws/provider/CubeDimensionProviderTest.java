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
package org.gbif.metrics.ws.provider;

import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.metrics.es.CountQuery;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.context.request.NativeWebRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Tests that the Jersey provider does it's job unpacking the HTTP params and constructing the
 * internal datacube read builder properly.
 */
@ExtendWith(MockitoExtension.class)
public class CubeDimensionProviderTest {

  private final UUID DS = UUID.randomUUID();

  @Mock private NativeWebRequest webRequestMock;

  // Builds an Address (package declarations necessary, as this is the INTERNAL one)
  private CountQuery getInternallyBuilt() {
    return new CountQuery()
        .withParameter(OccurrenceCube.BASIS_OF_RECORD.getKey(), BasisOfRecord.OBSERVATION.name())
        .withParameter(OccurrenceCube.TAXON_KEY.getKey(), "212")
        .withParameter(OccurrenceCube.DATASET_KEY.getKey(), DS.toString());
  }

  // An address built by the Jersey provider must be the same as a DC readbuilder version
  @Test
  public void testBuild() {
    CountQueryArgumentResolver b = new CountQueryArgumentResolver();
    Map<String, String[]> parameters = new HashMap<>();
    parameters.put(
        OccurrenceCube.BASIS_OF_RECORD.getKey(), new String[] {BasisOfRecord.OBSERVATION.name()});
    parameters.put(OccurrenceCube.TAXON_KEY.getKey(), new String[] {"212"});
    parameters.put(OccurrenceCube.DATASET_KEY.getKey(), new String[] {DS.toString()});

    when(webRequestMock.getParameterMap()).thenReturn(parameters);

    CountQuery countQuery = ((CountQuery) b.resolveArgument(null, null, webRequestMock, null));
    assertEquals(getInternallyBuilt(), countQuery);
  }
}
