package org.gbif.metrics.cube.tile.density;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.metrics.cube.tile.density.DensityCubeUtil.Op;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class DensityTileTest {

  @Test
  public void cellId() {
    assertEquals(10, DensityTile.toCellId(129, 129, 64));
    assertEquals(36, DensityTile.toCellId(129, 129, 32));
  }

  /**
   * Simple API illustration
   */
  @Test
  public void testCell() {

    DensityTile tile =
      DensityTile.builder(0, 0, 0, 64)
        // Z, X, Y, ClusterSize
        .collect(Layer.SP_NO_YEAR, -0.1d, 0.1d, 1)
        // lat, lng, count
        .collect(Layer.SP_2000_2010, -0.1d, 0.1d, 1000).collect(Layer.OBS_PRE_1900, -0.1d, 0.1d, 1)
        .collect(Layer.SP_NO_YEAR, -0.1d, 0.1d, 2).build();

    assertEquals(3, tile.cell(Layer.SP_NO_YEAR, 2, 2));
    assertEquals(0, tile.cell(Layer.SP_NO_YEAR, 0, 0));
    assertEquals(1, tile.cell(Layer.OBS_PRE_1900, 2, 2));
    assertEquals(1000, tile.cell(Layer.SP_2000_2010, 2, 2));
    assertEquals(3, tile.layers().size());

    try {
      byte[] b = tile.serialize();
      tile = DensityTile.DESERIALIZE(b);
      assertEquals(3, tile.cell(Layer.SP_NO_YEAR, 2, 2));
      assertEquals(0, tile.cell(Layer.SP_NO_YEAR, 0, 0));
      assertEquals(1, tile.cell(Layer.OBS_PRE_1900, 2, 2));
      assertEquals(1000, tile.cell(Layer.SP_2000_2010, 2, 2));
      assertEquals(3, tile.layers().size());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testDownscale() {
    Map<Layer, Map<Integer, Integer>> layers =
      ImmutableMap.<Layer, Map<Integer, Integer>>builder()
        .put(Layer.FOSSIL, ImmutableMap.<Integer, Integer>of(514, 1, 515, 1))
        .build();
    DensityTile tile1px = new DensityTile(0, 0, 0, 1, layers);

    DensityTile tile2px = tile1px.downscale(2);
    assertEquals("Should still have only 1 layer", 1, tile2px.layers().size());
    assertEquals("Downsizing should have merged adjacent cells", 1, tile2px.layers().get(Layer.FOSSIL).size());
    assertEquals("Downsizing should have accumulated adjacent counts", Integer.valueOf(2),
      tile2px.layers().get(Layer.FOSSIL).get(129)); // the new key

    DensityTile tile4px = tile1px.downscale(4);
    assertEquals("Should still have only 1 layer", 1, tile4px.layers().size());
    assertEquals("Downsizing should have merged adjacent cells", 1, tile4px.layers().get(Layer.FOSSIL).size());
    assertEquals("Downsizing should have accumulated adjacent counts", Integer.valueOf(2),
      tile4px.layers().get(Layer.FOSSIL).get(0)); // the new key

    // test the 2-4 downscale
    tile4px = tile2px.downscale(4);
    assertEquals("Should still have only 1 layer", 1, tile4px.layers().size());
    assertEquals("Downsizing should have merged adjacent cells", 1, tile4px.layers().get(Layer.FOSSIL).size());
    assertEquals("Downsizing should have accumulated adjacent counts", Integer.valueOf(2),
      tile4px.layers().get(Layer.FOSSIL).get(0)); // the new key
  }

  @Test
  public void testDelete() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      Batch<DensityTile> update = new Batch<DensityTile>();
      Occurrence occurrence =
        mapper
          .readValue(
            "{\"key\":307075245,\"kingdom\":\"Plantae\",\"phylum\":\"Magnoliophyta\",\"clazz\":\"Magnoliopsida\",\"order\":\"Lamiales\",\"family\":\"Lamiaceae\",\"genus\":\"Stachys\",\"subgenus\":null,\"species\":\"Stachys palustris\",\"kingdomKey\":6,\"phylumKey\":49,\"classKey\":220,\"orderKey\":408,\"familyKey\":2497,\"genusKey\":2927228,\"subgenusKey\":null,\"speciesKey\":2927245,\"datasetKey\":\"086a644d-6cbe-43b7-b7c7-d33d36028d7f\",\"publishingOrgKey\":\"07f617d0-c688-11d8-bf62-b8a03c50a862\",\"scientificName\":\"Stachys palustris L.\",\"taxonKey\":2927245,\"basisOfRecord\":\"UNKNOWN\",\"longitude\":-5.54387,\"latitude\":55.28758,\"stateProvince\":null,\"country\":\"GB\",\"continent\":\"EUROPE\",\"year\":1997,\"month\":null,\"day\":null,\"modified\":1368692609000,\"protocol\":\"DWC_ARCHIVE\",\"publishingCountry\":\"GB\"}",
            Occurrence.class);
      occurrence.getFields().put(DwcTerm.occurrenceID, null);
      occurrence.getFields().put(DwcTerm.institutionCode, "Botanical Society of the British Isles");
      occurrence.getFields().put(DwcTerm.collectionCode, "6340");
      occurrence.getFields().put(DwcTerm.catalogNumber, "59935182");
      int zoom = 5;
      int pixels = 1;
      int iterations = 10;
      for (int i = 0; i < iterations; i++) {
        update.putAll(DensityCubeUtil.cubeMutations(occurrence, Op.ADDITION, zoom, pixels));
      }

      for (Entry<Address, DensityTile> e : update.getMap().entrySet()) {
        // accumulate the count of the tile and ensure it has data
        assertTrue("Tile should have content", totalCount(e.getValue()) > 0);
      }
      for (int i = 0; i < iterations; i++) {
        update.putAll(DensityCubeUtil.cubeMutations(occurrence, Op.SUBTRACTION, zoom, pixels));
      }
      for (Entry<Address, DensityTile> e : update.getMap().entrySet()) {
        // accumulate the count of the tile and ensure it has data
        assertTrue("Tile should have no content", totalCount(e.getValue()) == 0);
      }


    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private final int totalCount(DensityTile tile) {
    int total = 0;
    for (Entry<Layer, Map<Integer, Integer>> e : tile.layers().entrySet()) {
      for (Integer count : e.getValue().values()) {
        total += count;
      }
    }
    return total;
  }
}
