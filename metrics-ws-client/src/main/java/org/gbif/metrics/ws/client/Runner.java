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

import org.gbif.api.model.metrics.cube.Dimension;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.vocabulary.Country;
import org.gbif.ws.client.ClientFactory;

import java.util.UUID;

public class Runner {

  public static void main(String[] args) {
    ClientFactory clientFactoryDev = new ClientFactory("http://api.gbif-dev.org/v1/");
    CubeWsClient cubeWsClientDev = clientFactoryDev.newInstance(CubeWsClient.class);

    ClientFactory clientFactoryLocal = new ClientFactory("http://localhost:8080");
    CubeWsClient cubeWsClientLocal = clientFactoryLocal.newInstance(CubeWsClient.class);

    //    List<Rollup> schema = cubeWsClientLocal.getSchema();
    //    System.out.println(schema);

    ReadBuilder readBuilder = new ReadBuilder();
    readBuilder.at(new Dimension<>("country", Country.class), Country.DENMARK);
    long l = cubeWsClientDev.get(readBuilder);
    System.out.println(l);

    ReadBuilder readBuilder1 = new ReadBuilder();
    readBuilder1.at(
        new Dimension<>("datasetKey", UUID.class),
        UUID.fromString("3ad913f9-2f86-48c7-b570-3ec9f71acf88"));
    long l1 = cubeWsClientDev.get(readBuilder1);
    System.out.println(l1);
  }
}
