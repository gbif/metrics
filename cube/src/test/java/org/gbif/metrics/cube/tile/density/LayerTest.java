package org.gbif.metrics.cube.tile.density;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.metrics.cube.mapred.OccurrenceWritable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class LayerTest {

  @Test
  public void testInferFrom() {
    OccurrenceWritable o = new OccurrenceWritable();
    o.setBasisOfRecord(BasisOfRecord.LIVING_SPECIMEN);
    assertEquals(Layer.LIVING, Layer.inferFrom(o));
    o.setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION);
    assertEquals(Layer.OBS_NO_YEAR, Layer.inferFrom(o));
    o.setYear(1800);
    assertEquals(Layer.OBS_PRE_1900, Layer.inferFrom(o));
    o.setYear(1900);
    assertEquals(Layer.OBS_1900_1910, Layer.inferFrom(o));
    o.setYear(1920);
    assertEquals(Layer.OBS_1920_1930, Layer.inferFrom(o));
    o.setYear(1929);
    assertEquals(Layer.OBS_1920_1930, Layer.inferFrom(o));
    o.setYear(2012);
    assertEquals(Layer.OBS_2010_2020, Layer.inferFrom(o));
    o.setBasisOfRecord(BasisOfRecord.OBSERVATION);
    assertEquals(Layer.OBS_2010_2020, Layer.inferFrom(o));
    o.setYear(2020);
    assertEquals(Layer.OBS_NO_YEAR, Layer.inferFrom(o));
  }
}
