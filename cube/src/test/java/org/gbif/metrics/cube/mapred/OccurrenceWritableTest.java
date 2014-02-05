package org.gbif.metrics.cube.mapred;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OccurrenceWritableTest {

  static final UUID testUuid = UUID.randomUUID();

  @Test
  public void testSerDe() {
    OccurrenceWritable o = buildOcc();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    try {
      o.write(out);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      o = new OccurrenceWritable();
      o.readFields(new DataInputStream(bais));
      assertEquals(new Integer(1), o.getKingdomKey());
      assertEquals(new Integer(2012), o.getYear());
      assertEquals(new Integer(12), o.getMonth());
      assertEquals(Country.UNITED_KINGDOM, o.getPublishingCountry());
      assertNull(o.getPhylumKey());
      assertEquals(0.89d, o.getLatitude().doubleValue(), 0.0001);
      assertNull(o.getLongitude());
      assertEquals(BasisOfRecord.FOSSIL_SPECIMEN, o.getBasisOfRecord());
      assertEquals(EndpointType.DWC_ARCHIVE, o.getProtocol());

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
  private OccurrenceWritable buildOcc() {
    OccurrenceWritable ow = new OccurrenceWritable();
    ow.setTaxonKey(1);
    ow.setKingdomKey(1);
    ow.setDatasetKey(testUuid);
    ow.setPublishingCountry(Country.UNITED_KINGDOM);
    ow.setBasisOfRecord(BasisOfRecord.FOSSIL_SPECIMEN);
    ow.setProtocol(EndpointType.DWC_ARCHIVE);
    ow.setLatitude(0.89);
    ow.setLongitude(null);
    ow.setYear(2012);
    ow.setMonth(12);
    ow.setCount(1);
    return ow;
  }

  @Test
  public void testCompareTo() {
    OccurrenceWritable o1 = buildOcc();
    OccurrenceWritable o2 = buildOcc();
    OccurrenceWritable o3 = buildOcc();
    o3.setOrderKey(10);
    assertEquals(0, o1.compareTo(o2));
    assertTrue(o1.compareTo(o3) > 0);
    assertTrue(o3.compareTo(o2) < 0);
  }
}
