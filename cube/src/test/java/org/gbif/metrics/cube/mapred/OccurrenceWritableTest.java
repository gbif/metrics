package org.gbif.metrics.cube.mapred;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import com.google.common.collect.Sets;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OccurrenceWritableTest {

  static final UUID testUuid = UUID.randomUUID();

  @Test
  public void testSerDe() {
    OccurrenceWritable o =
      new OccurrenceWritable(1, null, null, null, null, null, null, null, 1, Sets.<OccurrenceIssue>newHashSet(),
                             null, testUuid, null, Country.UNITED_KINGDOM, 0.89, null, 2012, 12,
        BasisOfRecord.FOSSIL_SPECIMEN, EndpointType.DWC_ARCHIVE, 1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    try {
      o.write(out);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      o = new OccurrenceWritable();
      o.readFields(new DataInputStream(bais));
      assertEquals(new Integer(1), o.getKingdomID());
      assertEquals(new Integer(2012), o.getYear());
      assertEquals(new Integer(12), o.getMonth());
      assertEquals(Country.UNITED_KINGDOM, o.getPublishingCountry());
      assertNull(o.getPhylumID());
      assertNull(o.getLongitude());
      assertNull(o.getLongitude());
      assertEquals(BasisOfRecord.FOSSIL_SPECIMEN, o.getBasisOfRecord());
      assertEquals(EndpointType.DWC_ARCHIVE, o.getProtocol());

    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCompareTo() {
    OccurrenceWritable o1 =
      new OccurrenceWritable(1, null, null, null, null, null, null, null, 1, Sets.<OccurrenceIssue>newHashSet(), null, testUuid, null, Country.UNITED_KINGDOM, 0.89, null, 1, 12,
        BasisOfRecord.FOSSIL_SPECIMEN, EndpointType.DWC_ARCHIVE, 1);
    OccurrenceWritable o2 =
      new OccurrenceWritable(1, null, null, null, null, null, null, null, 1, Sets.<OccurrenceIssue>newHashSet(), null, testUuid, null, Country.UNITED_KINGDOM, 0.89, null, 1, 12,
        BasisOfRecord.FOSSIL_SPECIMEN, EndpointType.DWC_ARCHIVE, 1);
    OccurrenceWritable o3 =
      new OccurrenceWritable(1, null, null, 10, null, null, null, null, 1, Sets.<OccurrenceIssue>newHashSet(), null, testUuid, Country.UNITED_KINGDOM, null, 0.89, null, 1, 12,
        BasisOfRecord.FOSSIL_SPECIMEN, EndpointType.DWC_ARCHIVE, 1);
    assertEquals(0, o1.compareTo(o2));
    assertTrue(o1.compareTo(o3) > 0);
    assertTrue(o3.compareTo(o2) < 0);
  }
}
